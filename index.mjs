import debug from 'debug';
// import { JobQueue } from '@oada/oada-jobs';
// import fetch from 'node-fetch';
import XLSX from 'xlsx';
import Promise from 'bluebird';
import config from './config.js';
import moment from 'moment';
import commander from 'commander';
import client from '@oada/client';

const info = debug('report-gen:info');
const trace = debug('report-gen:trace');
const warn = debug('report-gen:warn');
const error = debug('report-gen:error');

// These can be overrided with -d and/or -t on command line
let TRELLIS_URL = `https://${config.get('domain')}`;
let TRELLIS_TOKEN = config.get('token');
// let fetchOptions = {
//   headers: {
//     Authorization: `Bearer ${TRELLIS_TOKEN}`, // resets below if command-line -t
//   },
// };

(async () => {
  let program = new commander.Command();
  program
    .option('-q, --queue <queue>', '`jobs` or `jobs-success`', 'jobs-success')
    .option('-s, --state <state>', 'only generate a jobs report', 'true')
    .option('-d --domain <domain>', 'domain without https', 'localhost')
    .option('-t --token <token>', 'token', 'god');
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
      `Setting NODE_TLS_REJECT_UNAUTHORIZED = 0 because domain is localhost`
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

  if (program.state.toLowerCase() === 'true') {
    // console.log(program.state);
    const shares = await getState(conn);
    createUserAccess(shares.tradingPartners);
    createDocumentShares(shares.documents);
  }

  trace(`Starting getShares`);
  let jobs = await getShares(conn, program.queue);
  createEventLog(jobs);
})();

async function getState(conn) {
  // const tradingPartners = await conn.get('/bookmarks/trellisfw/trading-partners');
  trace('Getting trading partner list');
  let tradingPartners;
  try {
    tradingPartners = await tryFetch(conn, {
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
        partner = await tryFetch(conn, {
          path: `/bookmarks/trellisfw/trading-partners/${pid}`,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to get trading partner ${pid} %O`, e);
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
    { concurrency: 10 }
  );

  trace('Getting COI list');
  let cois;
  try {
    cois = await tryFetch(conn, {
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
    { concurrency: 10 }
  );

  trace('Getting Audit Shares');
  let audits;
  try {
    audits = await tryFetch(conn, {
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
    cois = await tryFetch(conn, {
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
      vdoc = await tryFetch(conn, {
        path: `/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/cois/${coi}`,
      }).then((res) => res.data);
    } catch (e) {
      error(`Failed to fetch coi ${coi} %O`, e);
      return;
    }

    if (!vdoc.hasOwnProperty('_id')) {
      return;
    }

    if (tradingPartners[pid].documents[coi] === undefined) {
      tradingPartners[pid].documents[coi] = getCoiDetails(vdoc);
    }
  });
}

async function getPartnerAudits(conn, tradingPartners, pid) {
  let audits;
  try {
    audits = await tryFetch(conn, {
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
      vdoc = await tryFetch(conn, {
        path: `/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/fsqa-audits/${audit}`,
      }).then((res) => res.data);
    } catch (e) {
      error('Failed to fetch audit: ${audit} %O', e);
      return;
    }

    if (!vdoc.hasOwnProperty('_id')) {
      return;
    }

    if (tradingPartners[pid].documents[audit] === undefined) {
      tradingPartners[pid].documents[audit] = getAuditDetails(vdoc);
    }
  });
}

async function getCoiShares(conn, tradingPartners, cois, cid) {
  let vdoc;
  try {
    vdoc = await tryFetch(conn, {
      path: `/bookmarks/trellisfw/cois/${cid}`,
    }).then((res) => res.data);
  } catch (e) {
    error(`Failed to get coi: ${cid} %O`, e);
    return;
  }

  if (!vdoc.hasOwnProperty('_id')) {
    return;
  }

  cois[cid] = { ...getCoiDetails(vdoc), shares: {} };
  Object.keys(tradingPartners)
    .filter((pid) => {
      tradingPartners[pid].hasOwnProperty(cid);
    })
    .forEach((pid) => {
      cois[cid].shares[pid] = {
        'trading partner name': tradingPartners[pid]['trading partner name'],
        'trading partner masterid':
          tradingPartners[pid]['trading partner masterid'],
      };
    });
  return;
}

async function getAuditShares(conn, tradingPartners, audits, aid) {
  let vdoc;
  try {
    vdoc = await tryFetch(conn, {
      path: `/bookmarks/trellisfw/fsqa-audits/${aid}`,
    }).then((res) => res.data);
  } catch (e) {
    error(`Failed to get coi: ${aid} %O`, e);
    return;
  }

  if (!vdoc.hasOwnProperty('_id')) {
    return;
  }

  audits[aid] = { ...getAuditDetails(vdoc), shares: {} };
  Object.keys(tradingPartners)
    .filter((pid) => {
      tradingPartners[pid].hasOwnProperty(aid);
    })
    .forEach((pid) => {
      audits[aid].shares[pid] = {
        'trading partner name': tradingPartners[pid]['trading partner name'],
        'trading partner masterid':
          tradingPartners[pid]['trading partner masterid'],
      };
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
  let jobs;
  try {
    trace(`Getting ${queue} list`);
    jobs = await tryFetch(conn, {
      path: `/bookmarks/services/trellis-shares/${queue}`,
    }).then((res) => res.data);
  } catch (e) {
    error('failed to get trellis shares %O', e);
    return;
  }
  if (!jobs.hasOwnProperty('_id')) {
    return;
  }
  delete jobs._id;
  delete jobs._meta;
  delete jobs._rev;
  delete jobs._type;

  switch (queue) {
    case 'jobs':
      return getJobsFuture(conn, jobs);
    case 'jobs-success':
      return getJobsSuccess(conn, jobs, dates);
  }
}

async function getJobsFuture(conn, jobs) {
  return Promise.map(
    Object.keys(jobs),
    async (sid) => {
      trace(`Getting data for share id: ${sid}`);
      let share;
      try {
        share = await tryFetch(conn, {
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
        vdoc = await tryFetch(conn, {
          path: share.config.src,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch document shared in job ${sid} %O: %O`, share, e);
        return;
      }
      if (!vdoc.hasOwnProperty('_id')) {
        return;
      }

      let partner;
      try {
        partner = await tryFetch(conn, {
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
    { concurrency: 10 }
  );
}

async function getJobsSuccess(conn, jobs, dates) {
  if (dates === undefined || dates.length === 0) {
    dates = [
      moment
        .max(Object.keys(jobs['day-index']).map((day) => moment(day)))
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
        return jobs['day-index'].hasOwnProperty(day);
      }),
    async (day) => {
      info(`Getting trellis shares for ${day}`);
      let shares;
      try {
        shares = await tryFetch(conn, {
          path: `/bookmarks/services/trellis-shares/jobs-success/day-index/${day}`,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to get shares for day ${day} %O`, e);
        return;
      }

      if (!shares.hasOwnProperty('_id')) {
        return;
      }

      delete shares._id;
      delete shares._rev;
      delete shares._type;
      delete shares._meta;

      let completed = await getFinishedShares(conn, shares, day);
      trace(`complete tasks for ${day} %O`, completed);
      // completed.concat(await getShareFail(shares));
      // completed.concat(await getEmailSuccess(shares));
      // completed.concat(await getEmailFail(shares));
      return completed;
    },
    { concurrency: 10 }
  );
}

async function getFinishedShares(conn, shares, day) {
  return Promise.map(
    Object.keys(shares),
    async (sid) => {
      trace(`Getting data for share id: ${sid}`);
      let share;
      try {
        share = await tryFetch(conn, {
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
        vdoc = await tryFetch(conn, {
          path: share.config.src,
        }).then((res) => res.data);
      } catch (e) {
        error(`Failed to fetch document shared in job ${sid} %O: %O`, share, e);
        return;
      }
      if (!vdoc.hasOwnProperty('_id')) {
        return;
      }

      let partner;
      try {
        partner = await tryFetch(conn, {
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
        'event time': moment(
          Object.values(share.updates)
            .filter((s) => s.status === 'success')
            .map((s) => s.time)
            .shift()
        ).format('MM/DD/YYYY hh:mm'),
        'event type': 'share',
        ...details,
      };
    },
    { concurrency: 10 }
  );
}

function getCoiDetails(vdoc) {
  try {
    return {
      'document type': 'coi',
      'document id': vdoc._id,
      'document name': vdoc.certificate.file_name,
      'upload date': moment(vdoc.certificate.docdate).format('MM/DD/YYYY'),
      'coi holder': vdoc.holder.name,
      'coi producer': vdoc.producer.name,
      'coi insured': vdoc.insured.name,
      'coi expiration date': moment
        .min(
          Object.values(vdoc.policies).map((policy) => {
            return moment(policy.expire_date);
          })
        )
        .format('MM/DD/YYYY'),
      'audit organization': '',
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
      'upload date': '',
      'coi holder': '',
      'coi producer': '',
      'coi insured': '',
      'coi expiration date': '',
      'audit organization name': vdoc.organization.name,
      'audit expiration date': moment(
        vdoc.certificate_validity_period.end,
        'MM/DD/YYYY'
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
async function createDocumentShares(data) {
  let docs = [];
  Object.values(data)
    .filter((doc) => doc.hasOwnProperty('shares'))
    .forEach((doc) => {
      const pids = Object.keys(doc.shares);
      const d = {
        'document name': doc['document name'],
        'document id': doc['document id'],
        'document type': doc['document type'],
        'trading partner masterid': '',
        'trading partner name': '',
        'upload date': doc['upload date'],
        'coi holder': doc['coi holder'],
        'coi producer': doc['coi producer'],
        'coi insured': doc['coi insured'],
        'coi expiration date': doc['coi expiration date'],
        'audit organization': doc['audit organization'],
        'audit expiration date': doc['audit expiration date'],
        'audit score': doc['audit score'],
      };
      if (pids.length === 0) {
        docs.push({ ...d });
      } else {
        pids.forEach((pid) => {
          docs.push({
            ...d,
            'trading partner name': doc.shares[pid]['trading partner name'],
            'trading partner masterid':
              doc.shares[pid]['trading partner masterid'],
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
      'audit organization',
      'audit expiration date',
      'audit score',
    ],
  });
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws);
  XLSX.writeFile(wb, `${moment().format('YYYY-MM-DD')}_document_shares.xlsx`);
  trace('Document share report written');
  return;
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
        users.push({ ...tradingPartners[pid].documents[doc], ...props });
      });
    }
  });

  // trace('user access %O', users);
  trace('Generating user access report');
  const ws = XLSX.utils.json_to_sheet(users, {
    Headers: [
      'trading partner masterid',
      'trading partner name',
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
  XLSX.writeFile(wb, `${moment().format('YYYY-MM-DD')}_user_access.xlsx`);
  trace('User access report written');
  return;
}

function createEventLog(data) {
  trace('event log %O', data.trellisShares);
  trace('Generating event report');
  const ws = XLSX.utils.json_to_sheet(
    data.trellisShares.filter((d) => d !== null && d !== undefined),
    {
      Headers: [
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
    }
  );
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws);
  XLSX.writeFile(wb, `${moment().format('YYYY-MM-DD')}_event_log.xlsx`);
  trace('Event log report written');
  return;
}

/*
function createSheets(shares, events) {
  const date = moment().format('YYYY-MM-DD');
  info('Creating document share report');
  let docShares = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(
    docShares,
    createDocumentShares(shares.documents)
  );
  XLSX.writeFile(docShares, `${date}_document_shares.xlsx`);
  info('Document share report written');
 
  // info('Creating user access report');
  // let userAccess = XLSX.utils.book_new();
  // XLSX.utils.book_append_sheet(
  //   userAccess,
  //   createUserAccess(shares.tradingPartners)
  // );
  // XLSX.writeFile(userAccess, `${date}_user_access.xlsx`);
  // info('User access report written');
 
  // info('Creating event log report');
  // let eventLog = XLSX.utils.book_new();
  // XLSX.utils.book_append_sheet(eventLog, createEventLog(events));
  // XLSX.writeFile(eventLog, `${date}_event_log.xlsx`);
  // info('Event log written');
}
*/

async function tryFetch(conn, opt) {
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
