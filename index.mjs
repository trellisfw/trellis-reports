import debug from 'debug';
// import { JobQueue } from '@oada/oada-jobs';
import fetch from 'node-fetch';
import XLSX from 'xlsx';
import Promise from 'bluebird';
import config from './config.js';
import moment from 'moment';
import commander from 'commander';
import client from '@oada/client';
import fs from 'fs';

const info = debug('report-gen:info');
const trace = debug('report-gen:trace');
const warn = debug('report-gen:warn');
const error = debug('report-gen:error');

// These can be overrided with -d and/or -t on command line
let TRELLIS_URL = `https://${config.get('domain')}`;
let TRELLIS_TOKEN = config.get('token');

(async () => {
  let program = new commander.Command();
  program
    .option('-q, --queue <queue>', '`waiting` or `complete`', 'complete')
    .option(
      '-s, --state <state>',
      'whether or not to generate the current state report',
      'true',
    )
    .option('-d --domain <domain>', 'domain without https')
    .option('-t --token <token>', 'token')
    .option(
      '-f, --file <file>',
      'location to save reports, if none specified will upload to <domain>',
    );
  program.parse(process.argv);

  if (program.domain) {
    program.domain = program.domain.replace(/^https:\/\//, ''); // tolerate if they put the https on the front
    TRELLIS_URL = 'https://' + program.domain;
    trace(`Using command-line domain, final domain is: ${TRELLIS_URL}`);
  }
  if (program.token) {
    TRELLIS_TOKEN = program.token;
    // fetchOptions.headers.Authorization = `Bearer ${TRELLIS_TOKEN}`; // need to reset
    trace(`Using command-line token, final token is: ${TRELLIS_TOKEN}`);
  }

  if (TRELLIS_URL === 'https://localhost') {
    trace(
      `Setting NODE_TLS_REJECT_UNAUTHORIZED = 0 because domain is localhost`,
    );
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;
  }

  let conn;
  try {
    conn = await client.connect({
      domain: TRELLIS_URL,
      token: TRELLIS_TOKEN,
      // concurrency: 10,
    });
  } catch (e) {
    error('Failed to open connection %O', e);
    return;
  }

  let userAccess;
  let documentShares;
  if (program.state.toLowerCase() === 'true') {
    // console.log(program.state);
    trace('Getting share state');
    const shares = await getState(conn);
    userAccess = createUserAccess(shares.tradingPartners);
    documentShares = createDocumentShares(shares.documents);
  }

  trace(`Starting getShares`);
  let jobs = await getShares(conn, program.queue);
  const eventLog = createEventLog(jobs);
  if (program.file) {
    saveReports(
      userAccess,
      documentShares,
      eventLog,
      // program.queue,
      program.file,
    );
  } else {
    await uploadReports(
      conn,
      eventLog,
      userAccess,
      documentShares,
      program.queue,
    );
  }
})();

async function getState(conn) {
  // const tradingPartners = await conn.get('/bookmarks/trellisfw/trading-partners');
  trace('Getting trading partner list');
  let tradingPartners;
  try {
    tradingPartners = await tryFetchGet(conn, {
      path: '/bookmarks/trellisfw/trading-partners',
    }).then((res) => res.data);
  } catch (e) {
    error('Failed to get list of trading partners %O', e);
    return;
  }
  // is there a better way to avoid these?
  if (!tradingPartners.hasOwnProperty('_id')) {
    return { tradingPartners: {}, documents: {} };
  }

  delete tradingPartners._id;
  delete tradingPartners._rev;
  delete tradingPartners._type;
  delete tradingPartners._meta;
  delete tradingPartners['masterid-index'];
  delete tradingPartners['expand-index'];

  await Promise.map(
    Object.keys(tradingPartners),
    async (pid) => {
      info(`Getting documents for trading partner ${pid}`);
      let partner;
      try {
        partner = await tryFetchGet(conn, {
          path: `/bookmarks/trellisfw/trading-partners/${pid}`,
        }).then((res) => res.data);
      } catch (e) {
        if (e.status === 404) {
          info(`Trading partner ${pid} has no documents`);
        } else {
          error(`Failed to get trading partner ${pid} %O`, e);
        }
        return;
      }

      if (!partner.hasOwnProperty('_id')) {
        return;
      }

      tradingPartners[pid] = {
        'trading partner name': partner.name,
        'trading partner masterid': partner.masterid,
        documents: {},
      };

      // these add to currentShares
      await getPartnerCois(conn, tradingPartners, pid);
      await getPartnerAudits(conn, tradingPartners, pid);
    },
    { concurrency: 10 },
  );

  trace('Getting COI list');
  let cois;
  try {
    cois = await tryFetchGet(conn, {
      path: `/bookmarks/trellisfw/cois`,
    }).then((res) => res.data);
  } catch (e) {
    error('Failed to get list of COIs %O', e);
    return { tradingPartners, documents: {} };
  }

  // trace('COIs: %O', cois);
  if (!cois.hasOwnProperty('_id')) {
    return { tradingPartners, documents: {} };
  }
  trace('Getting COI Shares');
  delete cois._id;
  delete cois._rev;
  delete cois._meta;
  delete cois._type;

  await Promise.map(
    Object.keys(cois),
    async (coi) => {
      trace(`Getting COI ${coi}`);
      await getCoiShares(conn, tradingPartners, cois, coi);
    },
    { concurrency: 10 },
  );

  trace('Getting Audit Shares');
  let audits;
  try {
    audits = await tryFetchGet(conn, {
      path: `/bookmarks/trellisfw/fsqa-audits`,
    }).then((res) => res.data);
  } catch (e) {
    error('Failed to get list of audits %O', e);
    return { tradingPartners, documents: { ...cois } };
  }

  if (audits.hasOwnProperty('_id')) {
    delete audits._id;
    delete audits._rev;
    delete audits._meta;
    delete audits._type;

    await Promise.map(Object.keys(audits), async (aid) => {
      trace(`Getting Audit ${aid}`);
      await getAuditShares(conn, tradingPartners, audits, aid);
    });
  }

  return { tradingPartners, documents: { ...cois, ...audits } };
}

async function getPartnerCois(conn, tradingPartners, pid) {
  let cois;
  try {
    cois = await tryFetchGet(conn, {
      path: `/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/cois`,
    }).then((res) => res.data);
  } catch (e) {
    error(`Failed to get list of COIs for partner ${pid}`, e);
    return;
  }

  if (!cois.hasOwnProperty('_id')) {
    return;
  }
  delete cois._id;
  delete cois._rev;
  delete cois._type;
  delete cois._meta;

  await Promise.each(Object.keys(cois), async (coi) => {
    trace(`Getting coi ${coi}`);
    let vdoc;
    try {
      vdoc = await tryFetchGet(conn, {
        path: `/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/cois/${coi}`,
      }).then((res) => res.data);
    } catch (e) {
      error(`Failed to fetch coi ${coi} %O`, e);
      return;
    }

    if (!vdoc.hasOwnProperty('_id')) {
      return;
    }
    try {
      vdoc._meta = await tryFetchGet(conn, {
        path: vdoc._meta._id,
      }).then((res) => res.data);
    } catch (e) {
      error(`Failed to get ${vdoc._id} _meta: %O`, e);
      return;
    }

    if (tradingPartners[pid].documents[vdoc._id] === undefined) {
      tradingPartners[pid].documents[vdoc._id] = getCoiDetails(vdoc);
    }
  });
}

async function getPartnerAudits(conn, tradingPartners, pid) {
  let audits;
  try {
    audits = await tryFetchGet(conn, {
      path: `/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/fsqa-audits`,
    }).then((res) => res.data);
  } catch (e) {
    error(`Failed to get list of audits for partner ${pid} %O`, e);
    return;
  }

  if (!audits.hasOwnProperty('_id')) {
    return;
  }
  delete audits._id;
  delete audits._rev;
  delete audits._type;
  delete audits._meta;

  Promise.each(Object.keys(audits), async (audit) => {
    let vdoc;
    try {
      vdoc = await tryFetchGet(conn, {
        path: `/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/fsqa-audits/${audit}`,
      }).then((res) => res.data);
    } catch (e) {
      error('Failed to fetch audit: ${audit} %O', e);
      return;
    }

    if (!vdoc.hasOwnProperty('_id')) {
      return;
    }
    try {
      vdoc._meta = await tryFetchGet(conn, {
        path: vdoc._meta._id,
      }).then((res) => res.data);
    } catch (e) {
      error(`Failed to get ${vdoc._id} _meta: %O`, e);
      return;
    }

    if (tradingPartners[pid].documents[vdoc._id] === undefined) {
      tradingPartners[pid].documents[vdoc._id] = getAuditDetails(vdoc);
    }
  });
}

async function getCoiShares(conn, tradingPartners, cois, cid) {
  let vdoc;
  try {
    vdoc = await tryFetchGet(conn, {
      path: `/bookmarks/trellisfw/cois/${cid}`,
    }).then((res) => res.data);
  } catch (e) {
    error(`Failed to get coi: ${cid} %O`, e);
    return;
  }

  if (!vdoc.hasOwnProperty('_id')) {
    return;
  }
  try {
    vdoc._meta = await tryFetchGet(conn, {
      path: vdoc._meta._id,
    }).then((res) => res.data);
  } catch (e) {
    error(`Failed to get ${vdoc._id} _meta: %O`, e);
    return;
  }

  cois[cid] = { ...getCoiDetails(vdoc), shares: {} };
  Object.keys(tradingPartners)
    // .filter((pid) => {
    //   tradingPartners[pid].documents.hasOwnProperty(vdoc._id);
    // })
    .forEach((pid) => {
      if (tradingPartners[pid].documents[vdoc._id] !== undefined) {
        trace(`coi ${cid} shared with ${pid}`);
        cois[cid].shares[pid] = {
          'trading partner name': tradingPartners[pid]['trading partner name'],
          'trading partner masterid':
            tradingPartners[pid]['trading partner masterid'],
        };
      }
    });
  return;
}

async function getAuditShares(conn, tradingPartners, audits, aid) {
  let vdoc;
  try {
    vdoc = await tryFetchGet(conn, {
      path: `/bookmarks/trellisfw/fsqa-audits/${aid}`,
    }).then((res) => res.data);
  } catch (e) {
    error(`Failed to get coi: ${aid} %O`, e);
    return;
  }

  if (!vdoc.hasOwnProperty('_id')) {
    return;
  }
  try {
    vdoc._meta = await tryFetchGet(conn, {
      path: vdoc._meta._id,
    }).then((res) => res.data);
  } catch (e) {
    error(`Failed to get ${vdoc._id} _meta: %O`, e);
    return;
  }

  audits[aid] = { ...getAuditDetails(vdoc), shares: {} };
  Object.keys(tradingPartners)
    // .filter((pid) => {
    //   tradingPartners[pid].documents.hasOwnProperty(vdoc._id);
    // })
    .forEach((pid) => {
      if (tradingPartners[pid].documents[vdoc._id] !== undefined) {
        trace(`Audit ${aid} shared with ${pid}`);
        audits[aid].shares[pid] = {
          'trading partner name': tradingPartners[pid]['trading partner name'],
          'trading partner masterid':
            tradingPartners[pid]['trading partner masterid'],
        };
      }
    });
  return;
}

async function getShares(conn, queue, dates) {
  trace('Get Share History');
  const trellisShares = await getTrellisShares(conn, queue, dates);
  const emailShares = {}; // await getEmailShares();
  trace('trellis shares: %O', trellisShares);
  return { trellisShares: trellisShares.flat(), emailShares };
}

async function getTrellisShares(conn, queue, dates) {
  switch (queue) {
    case 'waiting':
      let waiting;
      try {
        trace(`Getting ${queue} list`);
        waiting = await tryFetchGet(conn, {
          path: `/bookmarks/services/trellis-shares/jobs`,
        }).then((res) => res.data);
      } catch (e) {
        error('failed to get trellis shares %O', e);
        return;
      }
      if (!waiting.hasOwnProperty('_id')) {
        return;
      }
      delete waiting._id;
      delete waiting._meta;
      delete waiting._rev;
      delete waiting._type;

      return getJobsFuture(conn, waiting);

    case 'complete':
      let jobSuccess;
      try {
        trace(`Getting ${queue} list`);
        jobSuccess = await tryFetchGet(conn, {
          path: `/bookmarks/services/trellis-shares/jobs-success`,
        }).then((res) => res.data);
      } catch (e) {
        error('failed to get trellis shares %O', e);
        return;
      }
      if (!jobSuccess.hasOwnProperty('_id')) {
        return;
      }
      delete jobSuccess._id;
      delete jobSuccess._meta;
      delete jobSuccess._rev;
      delete jobSuccess._type;

      let jobFailure;
      try {
        trace(`Getting ${queue} list`);
        jobFailure = await tryFetchGet(conn, {
          path: `/bookmarks/services/trellis-shares/jobs-failure`,
        }).then((res) => res.data);
      } catch (e) {
        error('failed to get trellis shares %O', e);
        return;
      }
      if (!jobFailure.hasOwnProperty('_id')) {
        return;
      }
      delete jobFailure._id;
      delete jobFailure._meta;
      delete jobFailure._rev;
      delete jobFailure._type;

      trace('successful job days: %O', jobSuccess['day-index']);
      trace('failed job days: %O', jobFailure['day-index']);
      let complete = { ...jobSuccess['day-index'], ...jobFailure['day-index'] };
      // trace('completed job days: %O', jobFailure['day-index']);

      return getFinishedJobs(conn, complete, dates);
  }
}

async function getJobsFuture(conn, jobs) {
  return Promise.map(
    Object.keys(jobs),
    async (sid) => {
      trace(`Getting data for share id: ${sid}`);
      let share;
      try {
        share = await tryFetchGet(conn, {
          path: `${TRELLIS_URL}/bookmarks/services/trellis-shares/jobs/${sid}`,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch share ${sid} %O', e`);
        return;
      }
      // trace('share: %O', share);
      if (!share.hasOwnProperty('_id')) {
        return;
      }

      let vdoc;
      try {
        vdoc = await tryFetchGet(conn, {
          path: share.config.src,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch document shared in job ${sid} %O: %O`, share, e);
        return;
      }
      if (!vdoc.hasOwnProperty('_id')) {
        return;
      }
      try {
        vdoc._meta = await tryFetchGet(conn, {
          path: vdoc._meta._id,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to get ${vdoc._id} _meta: %O`, e);
        return;
      }

      let partner;
      try {
        partner = await tryFetchGet(conn, {
          path: share.config.chroot.split('/').slice(0, -2).join('/'),
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch partner in share job ${sid} %O', e`);
        return;
      }
      if (!partner.hasOwnProperty('_id')) {
        return;
      }

      let details;
      let partnerEmail;
      switch (share.config.doctype) {
        case 'cois':
          partnerEmail = partner['coi-emails'];
          details = getCoiDetails(vdoc);
          break;
        case 'audit':
          partnerEmail = partner['fsqa-emails'];
          details = getAuditDetails(vdoc);
          break;
      }

      return {
        'trading partner masterid': partner.masterid,
        'trading partner name': partner.name,
        'recipient email address': partnerEmail,
        'event time': 'awaiting approval',
        'event type': 'share',
        ...details,
      };
    },
    { concurrency: 10 },
  );
}

async function getFinishedJobs(conn, jobs, dates) {
  if (dates === undefined || dates.length === 0) {
    const today = moment();
    dates = [
      moment
        .max(
          Object.keys(jobs)
            .map((day) => moment(day))
            .filter((day) => day.isBefore(today)),
        )
        .format('YYYY-MM-DD'),
    ];
  }
  trace('dates for activities to be retrieved: %O', dates);
  trace('jobs days: %O', jobs['day-index']);

  return Promise.map(
    dates
      .map((day) => {
        trace(`day: ${day}`);
        return moment(day).format('YYYY-MM-DD');
      })
      .filter((day) => {
        trace(`day ${day}`);
        return jobs.hasOwnProperty(day);
      }),
    async (day) => {
      info(`Getting trellis shares for ${day}`);
      let success;
      try {
        success = await tryFetchGet(conn, {
          path: `/bookmarks/services/trellis-shares/jobs-success/day-index/${day}`,
        }).then((res) => res.data);
      } catch (e) {
        success = {};
        error(`Failed to get shares for day ${day} %O`, e);
      }

      if (success.hasOwnProperty('_id')) {
        delete success._meta;
        delete success._rev;
        delete success._type;
        delete success._id;
      }

      let failure;
      try {
        failure = await tryFetchGet(conn, {
          path: `/bookmarks/services/trellis-shares/jobs-failure/day-index/${day}`,
        }).then((res) => res.data);
      } catch (e) {
        failure = {};
        error(`Failed to get shares for day ${day} %O`, e);
      }

      if (failure.hasOwnProperty('_id')) {
        delete failure._meta;
        delete failure._rev;
        delete failure._type;
        delete failure._id;
      }

      let successful = await getSuccessShares(conn, success, day);
      let failures = await getFailureShares(conn, failure, day);
      let completed = successful.concat(failures);

      trace(`complete tasks for ${day} %O`, completed);
      // completed.concat(await getShareFail(shares));
      // completed.concat(await getEmailSuccess(shares));
      // completed.concat(await getEmailFail(shares));
      return completed;
    },
    { concurrency: 10 },
  );
}

async function getSuccessShares(conn, shares, day) {
  return Promise.map(
    Object.keys(shares),
    async (sid) => {
      trace(`Getting data for share id: ${sid}`);
      let share;
      try {
        share = await tryFetchGet(conn, {
          path: `/bookmarks/services/trellis-shares/jobs-success/day-index/${day}/${sid}`,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch share ${sid} %O`, e);
        return;
      }
      if (!share.hasOwnProperty('_id')) {
        return;
      }

      let vdoc;
      try {
        vdoc = await tryFetchGet(conn, {
          path: share.config.src,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch document shared in job ${sid} %O: %O`, share, e);
        return;
      }
      if (!vdoc.hasOwnProperty('_id')) {
        return;
      }
      try {
        vdoc._meta = await tryFetchGet(conn, {
          path: vdoc._meta._id,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to get ${vdoc._id} _meta: %O`, e);
        return;
      }

      let partner;
      try {
        partner = await tryFetchGet(conn, {
          path: share.config.chroot.split('/').slice(0, -2).join('/'),
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch partner in share job ${sid} %O', e`);
        return;
      }
      if (!partner.hasOwnProperty('_id')) {
        return;
      }

      let details;
      let partnerEmail;
      switch (share.config.doctype) {
        case 'cois':
          partnerEmail = partner['coi-emails'];
          details = getCoiDetails(vdoc);
          break;
        case 'audit':
          partnerEmail = partner['fsqa-emails'];
          details = getAuditDetails(vdoc);
          break;
      }

      return {
        'share status': 'success',
        ...details,
        'trading partner masterid': partner.masterid,
        'trading partner name': partner.name,
        'recipient email address': partnerEmail,
        'event time': moment(
          Object.values(share.updates)
            .filter((s) => s.status === 'success')
            .map((s) => s.time)
            .shift(),
        ).format('MM/DD/YYYY hh:mm'),
        'event type': 'share',
      };
    },
    { concurrency: 10 },
  );
}

async function getFailureShares(conn, shares, day) {
  return Promise.map(
    Object.keys(shares),
    async (sid) => {
      trace(`Getting data for share id: ${sid}`);
      let share;
      try {
        share = await tryFetchGet(conn, {
          path: `/bookmarks/services/trellis-shares/jobs-failure/day-index/${day}/${sid}`,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch share ${sid} %O`, e);
        return;
      }
      if (!share.hasOwnProperty('_id')) {
        return;
      }

      let vdoc;
      try {
        vdoc = await tryFetchGet(conn, {
          path: share.config.src,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch document shared in job ${sid} %O: %O`, share, e);
        return;
      }
      if (!vdoc.hasOwnProperty('_id')) {
        return;
      }
      try {
        vdoc._meta = await tryFetchGet(conn, {
          path: vdoc._meta._id,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to get ${vdoc._id} _meta: %O`, e);
        return;
      }

      let partner;
      try {
        partner = await tryFetchGet(conn, {
          path: share.config.chroot.split('/').slice(0, -2).join('/'),
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch partner in share job ${sid} %O', e`);
        return;
      }
      if (!partner.hasOwnProperty('_id')) {
        return;
      }

      let details;
      let partnerEmail;
      trace(`${share.config.doctype}`);
      switch (share.config.doctype) {
        case 'cois':
          partnerEmail = partner['coi-emails'];
          details = getCoiDetails(vdoc);
          break;
        case 'fsqa-audits':
          partnerEmail = partner['fsqa-emails'];
          details = getAuditDetails(vdoc);
          break;
      }

      return {
        'share status': 'failure',
        ...details,
        'trading partner masterid': partner.masterid,
        'trading partner name': partner.name,
        'recipient email address': partnerEmail,
        'event time': moment(
          Object.values(share.updates)
            .filter((s) => s.status === 'failure')
            .map((s) => s.time)
            .shift(),
        ).format('MM/DD/YYYY hh:mm'),
        'event type': 'share',
      };
    },
    { concurrency: 10 },
  );
}

function getCoiDetails(vdoc) {
  try {
    return {
      'document type': 'coi',
      'document id': vdoc._id,
      'document name': vdoc.certificate.file_name,
      'upload date': moment.unix(vdoc._meta.stats.created).format('MM/DD/YYYY'),
      'coi holder': vdoc.holder.name,
      'coi producer': vdoc.producer.name,
      'coi insured': vdoc.insured.name,
      'coi expiration date': moment
        .min(
          Object.values(vdoc.policies).map((policy) => {
            return moment(policy.expire_date);
          }),
        )
        .format('MM/DD/YYYY'),
      'audit organization name': '',
      'audit expiration date': '',
      'audit score': '',
    };
  } catch (e) {
    error(`Failed to get coi ${vdoc._id} details %O`);
    return;
  }
}

function getAuditDetails(vdoc) {
  try {
    trace(`date: ${vdoc.certificate_validity_period.end}`);
    return {
      'document id': vdoc._id,
      'document type': 'audit',
      'document name': `${vdoc.scheme.name} Audit - ${vdoc.organization.name}`,
      'upload date': moment.unix(vdoc._meta.stats.created).format('MM/DD/YYYY'),
      'coi holder': '',
      'coi producer': '',
      'coi insured': '',
      'coi expiration date': '',
      'audit organization name': vdoc.organization.name,
      'audit expiration date': moment(
        vdoc.certificate_validity_period.end,
        'MM/DD/YYYY',
      ).format('MM/DD/YYYY'),
      'audit score': `${vdoc.score.final.value} ${vdoc.score.final.units}`,
    };
  } catch (e) {
    error(`Failed to get audit ${vdoc._id} details %O`);
    return;
  }
}

// XXX Ensure all documents are listed even if no trading partner has access
// to them
//
// build map in memory while construction "user access"
function createDocumentShares(data) {
  let docs = [];
  Object.values(data).forEach((doc) => {
    const pids = Object.keys(doc.shares);
    if (pids.length === 0) {
      docs.push({
        'document name': doc['document name'],
        'document id': doc['document id'],
        'document type': doc['document type'],
        'upload date': doc['upload date'],
        'trading partner name': '',
        'trading partner masterid': '',
        'coi holder': doc['coi holder'],
        'coi producer': doc['coi producer'],
        'coi insured': doc['coi insured'],
        'coi expiration date': doc['coi expiration date'],
        'audit organization name': doc['audit organization name'],
        'audit expiration date': doc['audit expiration date'],
        'audit score': doc['audit score'],
      });
    } else {
      pids.forEach((pid) => {
        docs.push({
          'document name': doc['document name'],
          'document id': doc['document id'],
          'document type': doc['document type'],
          'upload date': doc['upload date'],
          'trading partner name': doc.shares[pid]['trading partner name'],
          'trading partner masterid':
            doc.shares[pid]['trading partner masterid'],
          'coi holder': doc['coi holder'],
          'coi producer': doc['coi producer'],
          'coi insured': doc['coi insured'],
          'coi expiration date': doc['coi expiration date'],
          'audit organization name': doc['audit organization name'],
          'audit expiration date': doc['audit expiration date'],
          'audit score': doc['audit score'],
        });
      });
    }
  });

  trace('document shares: %O', docs);
  trace('Generating document share report');
  const ws = XLSX.utils.json_to_sheet(docs, {
    Headers: [
      'document name',
      'document id',
      'document type',
      'trading partner name',
      'trading partner masterid',
      'upload date',
      'coi holder',
      'coi producer',
      'coi insured',
      'coi expiration date',
      'audit organization name',
      'audit expiration date',
      'audit score',
    ],
  });
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws);
  return XLSX.write(wb, {
    type: 'buffer',
    bookType: 'xlsx',
    filename: `${moment().format('YYYY-MM-DD')}_document_shares.xlsx`,
    Props: {
      Title: `${moment().format('YYYY-MM-DD')}_document_shares.xlsx`,
    },
  });
}

// XXX Ensure all trading partners are listed even if they don't have access
// to any documents
//
// list all document of <doctype> that a trading partner can access
// `/bookmarks/trellisfw/trading-partners/<id>/bookmarks/trellisfw/<doctype>`
//    - cois
//    - fsqa-audits
//    - fsqa-certificates
//    - letters-of-guarantee
function createUserAccess(tradingPartners) {
  let users = [];
  Object.keys(tradingPartners).forEach((pid) => {
    const props = {
      'trading partner name': tradingPartners[pid]['trading partner name'],
      'trading partner masterid':
        tradingPartners[pid]['trading partner masterid'],
    };
    const docs = Object.keys(tradingPartners[pid].documents);
    if (docs.length === 0) {
      users.push(props);
    } else {
      docs.forEach((doc) => {
        users.push({ ...props, ...tradingPartners[pid].documents[doc] });
      });
    }
  });

  // trace('user access %O', users);
  trace('Generating user access report');
  const ws = XLSX.utils.json_to_sheet(users, {
    Headers: [
      'trading partner name',
      'trading partner masterid',
      'document type',
      'document id',
      'document name',
      'upload date',
      'coi holder',
      'coi producer',
      'coi insured',
      'coi expiration date',
      'audit organization name',
      'audit expiration date',
      'audit score',
    ],
  });
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws);
  return XLSX.write(wb, {
    type: 'buffer',
    bookType: 'xlsx',
    filename: `${moment().format('YYYY-MM-DD')}_user_access.xlsx`,
    Props: {
      Title: `${moment().format('YYYY-MM-DD')}_user_access.xlsx`,
    },
  });
  // trace('User access report written');
  return;
}

function createEventLog(data) {
  trace('event log %O', data.trellisShares);
  trace('Generating event report');
  const ws = XLSX.utils.json_to_sheet(
    data.trellisShares.filter((d) => d !== null && d !== undefined),
    {
      Headers: [
        'share status',
        'document id',
        'document name',
        'document type',
        'upload date',
        'coi expiration date',
        'coi holder',
        'coi producer',
        'coi insured',
        'audit organization name',
        'audit expiration date',
        'audit score',
        'trading partner masterid',
        'trading partner name',
        'recipient email address',
        'event time',
        'event type',
      ],
    },
  );
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws);
  return XLSX.write(wb, {
    type: 'buffer',
    bookType: 'xlsx',
    filename: `${moment().format('YYYY-MM-DD')}_event_log.xlsx`,
    Props: {
      Title: `${moment().format('YYYY-MM-DD')}_event_log.xlsx`,
    },
  });
}

function saveReports(
  userAccess,
  documentShares,
  eventLog,
  // queue,
  filename,
) {
  if (userAccess !== undefined) {
    trace('Writng User Access Report');
    fs.writeFile(`${filename}_user_access.xlsx`, userAccess, (err) => {
      if (err) {
        error('Failed to write User Access Report %O', err);
      } else {
        info('User Access Report Written');
      }
    });
  }

  if (documentShares !== undefined) {
    trace('Writng Document Share Report');
    fs.writeFile(`${filename}_document_shares.xlsx`, documentShares, (err) => {
      if (err) {
        error('Failed to write Document Share Report %O', err);
      } else {
        info('Document Share Report Written');
      }
    });
  }

  if (eventLog !== undefined) {
    trace('Writing Event Log Report');
    fs.writeFile(`${filename}_event_log.xlsx`, eventLog, (err) => {
      if (err) {
        error('Failed to write Event Log Report %O', err);
      } else {
        info('Event Log Report Written');
      }
    });
  }
}

async function uploadReports(
  conn,
  eventLog,
  userAccess,
  documentShares,
  queue,
) {
  try {
    await ensureDayIndex(conn);
  } catch (e) {
    error('Failed to ensure day index exists: %O', e);
    error('Saving documents to disk');
    saveReports(
      userAccess,
      documentShares,
      eventLog,
      moment().format('YYYY-MM-DD'),
    );
    return;
  }

  const today = moment().format('YYYY-MM-DD');
  let reports = {};

  if (userAccess) {
    try {
      const res = await fetch(`${TRELLIS_URL}/resources`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${TRELLIS_TOKEN}`,
          'Content-Type':
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        },
        body: userAccess,
      });
      if (res.ok) {
        const loc = res.headers.get('content-location').substr(1);
        trace(`user access report uploaded to ${loc}`);
        reports['current-tradingpartnershares'] = {
          _id: loc,
        };
      } else {
        error('failed to post user access report');
      }
    } catch (e) {
      error('Failed to upload user access report %O', e);
    }
  }

  if (documentShares) {
    try {
      const res = await fetch(`${TRELLIS_URL}/resources`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${TRELLIS_TOKEN}`,
          'Content-Type':
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        },
        body: documentShares,
      });
      if (res.ok) {
        const loc = res.headers.get('content-location').substr(1);
        trace(`document share report uploaded to ${loc}`);
        reports['current-shareabledocs'] = {
          _id: loc,
        };
      } else {
        error('failed to post document share report');
      }
    } catch (e) {
      error('Failed to upload document share report %O', e);
    }
  }

  try {
    const res = await fetch(`${TRELLIS_URL}/resources`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${TRELLIS_TOKEN}`,
        'Content-Type':
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      },
      body: eventLog,
    });
    if (res.ok) {
      const loc = res.headers.get('content-location').substr(1);
      trace(`event log report uploaded to ${loc}`);
      reports['event-log'] = {
        _id: loc,
      };
    } else {
      error('Failed to post event log');
    }
  } catch (e) {
    error('Failed to upload event log report %O', e);
  }

  let dayReportsLoc;
  try {
    const res = await conn.post({
      path: '/resources',
      data: reports,
    });
    if (res.headers.hasOwnProperty('content-location')) {
      dayReportsLoc = res.headers['content-location'].substr(1);
      trace(`day index posted to ${dayReportsLoc}`);
    } else {
      error('day report: no content location provided');
    }
  } catch (e) {
    error('Failed to post reports %O', e);
  }

  try {
    // TODO make something similar to tryFetchGet
    await conn.put({
      path: '/bookmarks/services/trellis-reports/reports/day-index',
      data: {
        [today]: {
          _id: dayReportsLoc,
        },
      },
    });
  } catch (e) {
    error('Failed to link report locations %O', e);
  }
}

async function ensureDayIndex(conn) {
  try {
    const res = await tryFetchGet(conn, {
      path: '/bookmarks/services/trellis-reports/reports/day-index',
    });
    if (res.status === 200) {
      return;
    }
  } catch (e) {
    if (e.status !== 404) {
      throw e;
    }
  }

  let trellisReportsLoc;
  try {
    const res = await conn.post({
      path: '/resources',
      data: {},
    });
    if (res.headers.hasOwnProperty('content-location')) {
      trellisReportsLoc = res.headers['content-location'].substr(1);
      trace(`trellis-reports posted to ${trellisReportsLoc}`);
    } else {
      error('trellis-reports: no content location provided');
    }
  } catch (e) {
    error('Failed to create trellis-reports service document %O', e);
    throw e;
  }

  let reportsLoc;
  try {
    const res = await conn.post({
      path: '/resources',
      data: {
        'day-index': {},
      },
    });
    if (res.headers.hasOwnProperty('content-location')) {
      reportsLoc = res.headers['content-location'].substr(1);
      trace(`reports posted to ${reportsLoc}`);
    } else {
      error('reports: no content location provided');
    }
  } catch (e) {
    error('Failed to create report document %O', e);
    throw e;
  }

  try {
    await conn.put({
      path: '/bookmarks/services',
      data: {
        'trellis-reports': {
          _id: trellisReportsLoc,
          _rev: 0,
        },
      },
    });
  } catch (e) {
    error('Failed to add trellis-reports to services: %O', e);
    throw e;
  }

  try {
    await conn.put({
      path: '/bookmarks/services/trellis-reports',
      data: {
        reports: {
          _id: reportsLoc,
          _rev: 0,
        },
      },
    });
  } catch (e) {
    error('Failed to add reports page to trellis-reports: %O', e);
    throw e;
  }
}

async function tryFetchGet(conn, opt) {
  for (let i = 0; i < 5; i++) {
    try {
      return await conn.get(opt);
    } catch (e) {
      // TODO may not need this while using @oada/client
      if (e.status === 404) {
        trace(`Document not found: ${opt.path}`);
        throw e;
      } else {
        trace('%O', e);
        trace(`${opt.path} Connection reset, retrying...`);
      }
    }
  }
}
