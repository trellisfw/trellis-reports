import debug from 'debug';
// import { JobQueue } from '@oada/oada-jobs';
import fetch from 'node-fetch';
import XLSX from 'xlsx';
import Promise from 'bluebird';
import config from './config.js';
import moment from 'moment';
import commander from 'commander';

const info = debug('report-gen:info');
const trace = debug('report-gen:trace');
const warn = debug('report-gen:warn');
const error = debug('report-gen:error');

const TRELLIS_URL = `https://${config.get('trellis_url')}`;
const TRELLIS_TOKEN = config.get('trellis_token');

const fetchOptions = {
  headers: {
    Authorization: `Bearer ${TRELLIS_TOKEN}`,
  },
};

async function getState(_conn) {
  // const tradingPartners = await conn.get('/bookmarks/trellisfw/trading-partners');
  let tradingPartners;
  try {
    tradingPartners = await tryFetch(
      `${TRELLIS_URL}/bookmarks/trellisfw/trading-partners`,
      fetchOptions
    ).then((res) => res.json());
  } catch (e) {
    error('Failed to get list of trading partners %O', e);
    return;
  }
  // is there a better way to avoid these?
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
        partner = await tryFetch(
          `${TRELLIS_URL}/bookmarks/trellisfw/trading-partners/${pid}`,
          fetchOptions
        ).then((res) => {
          if (res.status === 404) {
            warn(`Failed to fetch trading partner ${res.status}`);
          }
          return res.json();
        });
      } catch (e) {
        error(`Failed to get trading partner ${pid} %O`, e);
        return;
      }

      if (!partner.hasOwnProperty('_id')) {
        return;
      }

      tradingPartners[pid] = {
        'trading partner name': partner.name,
        'trading partner id': partner.masterid,
        documents: {},
      };

      // these add to currentShares
      await getPartnerCois(tradingPartners, pid);
      await getPartnerAudits(tradingPartners, pid);
    },
    { concurrency: 10 }
  );

  let cois;
  try {
    cois = await tryFetch(
      `${TRELLIS_URL}/bookmarks/trellisfw/cois`,
      fetchOptions
    ).then((res) => {
      if (res.status === 404) {
        warn(`Failed to fetch cois ${res.status}`);
      }
      return res.json();
    });
  } catch (e) {
    error('Failed to get list of COIs %O', e);
  }

  if (cois.hasOwnProperty('_id')) {
    delete cois._id;
    delete cois._rev;
    delete cois._meta;
    delete cois._type;

    await Promise.map(Object.keys(cois), async (coi) => {
      await getCoiShares(tradingPartners, cois, coi);
    });
  }

  let audits;
  try {
    audits = await tryFetch(
      `${TRELLIS_URL}/bookmarks/trellisfw/fsqa-audits`,
      fetchOptions
    ).then((res) => {
      if (res.status === 404) {
        warn(`Failed to fetch audits ${res.status}`);
      }
      return res.json();
    });
  } catch (e) {
    error('Failed to get list of audits %O', e);
  }

  if (audits.hasOwnProperty('_id')) {
    delete audits._id;
    delete audits._rev;
    delete audits._meta;
    delete audits._type;

    await Promise.map(Object.keys(audits), async (aid) => {
      await getCoiShares(tradingPartners, audits, aid);
    });
  }

  return { tradingPartners, documents: { ...cois, ...audits } };
}

async function getPartnerCois(tradingPartners, pid) {
  let cois;
  try {
    cois = await tryFetch(
      `${TRELLIS_URL}/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/cois`,
      fetchOptions
    ).then((res) => {
      if (res.status === 404) {
        warn(`Failed to fetch cois for partner ${pid} ${res.status}`);
      }
      return res.json();
    });
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
      vdoc = await tryFetch(
        `${TRELLIS_URL}/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/cois/${coi}`,
        fetchOptions
      ).then((res) => {
        if (res.status === 404) {
          warn(`Failed to fetch coi ${coi}`);
        }
        return res.json();
      });
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

async function getPartnerAudits(tradingPartners, pid) {
  let audits;
  try {
    audits = await tryFetch(
      `${TRELLIS_URL}/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/fsqa-audits`,
      fetchOptions
    ).then((res) => {
      if (res.status === 404) {
        warn(`Failed to fetch audits for partner ${pid} ${res.status}`);
      }
      return res.json();
    });
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
      vdoc = await tryFetch(
        `${TRELLIS_URL}/bookmarks/trellisfw/trading-partners/${pid}/user/bookmarks/trellisfw/fsqa-audits/${audit}`,
        fetchOptions
      ).then((res) => {
        if (res.status === 404) {
          warn(`Failed to fetch audit ${audit}`);
        }
        return res.json();
      });
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

async function getCoiShares(tradingPartners, cois, cid) {
  let vdoc;
  try {
    vdoc = await tryFetch(
      `${TRELLIS_URL}/bookmarks/trellisfw/cois/${cid}`,
      fetchOptions
    ).then((res) => {
      if (res.status === 404) {
        warn(`Failed to fetch coi ${cid}`);
      }
      return res.json();
    });
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
        'trading partner id': tradingPartners[pid]['trading partner id'],
      };
    });
  return;
}

async function getAuditShares(tradingPartners, audits, aid) {
  let vdoc;
  try {
    vdoc = await tryFetch(
      `${TRELLIS_URL}/bookmarks/trellisfw/fsqa-audits/${aid}`,
      fetchOptions
    ).then((res) => {
      if (res.status === 404) {
        warn(`Failed to fetch audit ${aid}`);
      }
      return res.json();
    });
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
        'trading partner id': tradingPartners[pid]['trading partner id'],
      };
    });
  return;
}

async function getShares(_conn, queue, dates) {
  trace('Get Share History');
  const trellisShares = await getTrellisShares({}, queue, dates);
  const emailShares = {}; // await getEmailShares();
  trace('trellis shares: %O', trellisShares);
  return { trellisShares: trellisShares.flat(), emailShares };
}

async function getTrellisShares(_conn, queue, dates) {
  let jobs;
  try {
    trace(`Getting ${queue} list`);
    jobs = await tryFetch(
      `${TRELLIS_URL}/bookmarks/services/trellis-shares/${queue}`,
      fetchOptions
    ).then((res) => {
      if (res.status === 404) {
        warn(`Failed to fetch job success day index list ${res.status}`);
      }
      return res.json();
    });
  } catch (e) {
    error('failed to get trellis shares %O', e);
    return;
  }

  if (!jobs.hasOwnProperty('_id')) {
    return;
  }

  switch (queue) {
    case 'jobs':
      return getJobsFuture(_conn, jobs);
    case 'jobs-success':
      return getJobsSuccess(_conn, jobs, dates);
  }
}

async function getJobsFuture(_conn, jobs) {
  return Promise.map(
    Object.keys(jobs),
    async (sid) => {
      trace(`Getting data for share id: ${sid}`);
      let share;
      try {
        share = await tryFetch(
          `${TRELLIS_URL}/bookmarks/services/trellis-shares/jobs-success/${sid}`,
          fetchOptions
        ).then((res) => res.json());
      } catch (e) {
        error(`Failed to fetch share ${sid} %O', e`);
        return;
      }

      let vdoc;
      try {
        vdoc = await tryFetch(
          `${TRELLIS_URL}${share.config.src}`,
          fetchOptions
        ).then((res) => res.json());
      } catch (e) {
        error(`Failed to fetch document shared in job ${sid} %O', e`);
        return;
      }

      let partner;
      try {
        partner = await tryFetch(
          `${TRELLIS_URL}${share.config.chroot
            .split('/')
            .slice(0, -2)
            .join('/')}`,
          fetchOptions
        ).then((res) => res.json());
      } catch (e) {
        error(`Failed to fetch partner in share job ${sid} %O', e`);
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
        'trading partner id': partner.masterid,
        'trading partner name': partner.name,
        'recipient email address': partnerEmail,
        'event time': moment(
          Object.values(share.updates)
            .filter((s) => s.status === 'start')
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

async function getJobsSuccess(_conn, jobs, dates) {
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
        shares = await tryFetch(
          `${TRELLIS_URL}/bookmarks/services/trellis-shares/jobs-success/day-index/${day}`,
          fetchOptions
        ).then((res) => {
          if (res.status === 404) {
            warn(`Failed to fetch shares for ${day} ${res.status}`);
          }
          return res.json();
        });
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

      let completed = await getFinishedShares(shares, day);
      trace(`complete tasks for ${day} %O`, completed);
      // completed.concat(await getShareFail(shares));
      // completed.concat(await getEmailSuccess(shares));
      // completed.concat(await getEmailFail(shares));
      return completed;
    },
    { concurrency: 10 }
  );
}

async function getFinishedShares(shares, day) {
  return Promise.map(
    Object.keys(shares),
    async (sid) => {
      trace(`Getting data for share id: ${sid}`);
      let share;
      try {
        share = await tryFetch(
          `${TRELLIS_URL}/bookmarks/services/trellis-shares/jobs-success/day-index/${day}/${sid}`,
          fetchOptions
        ).then((res) => res.json());
      } catch (e) {
        error(`Failed to fetch share ${sid} %O', e`);
        return;
      }

      let vdoc;
      try {
        vdoc = await tryFetch(
          `${TRELLIS_URL}${share.config.src}`,
          fetchOptions
        ).then((res) => res.json());
      } catch (e) {
        error(`Failed to fetch document shared in job ${sid} %O', e`);
        return;
      }

      let partner;
      try {
        partner = await tryFetch(
          `${TRELLIS_URL}${share.config.chroot
            .split('/')
            .slice(0, -2)
            .join('/')}`,
          fetchOptions
        ).then((res) => res.json());
      } catch (e) {
        error(`Failed to fetch partner in share job ${sid} %O', e`);
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
        'trading partner id': partner.masterid,
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
        vdoc.certificate_validity_period.end
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
        'trading partner id': '',
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
            'trading partner id': doc.shares[pid]['trading partner id'],
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
      'trading partner id',
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
      'trading partner id': tradingPartners[pid]['trading partner id'],
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
      'trading partner id',
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
  const ws = XLSX.utils.json_to_sheet(data.trellisShares, {
    Headers: [
      'document type',
      'share status',
      'document filename',
      'coi holder name',
      'coi producer name',
      'coi insured name',
      'upload date',
      'coi expiration date',
      'audit organization name',
      'audit expiration date',
      'audit score',
      'trading partner name',
      'trading partner masterid',
      'email address',
      'trellisid',
      'event time',
      'event type',
    ],
  });
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

async function tryFetch(url, opt) {
  for (let i = 0; i < 5; i++) {
    try {
      return await fetch(url, opt);
    } catch (e) {
      if (!(e.code === 'ECONNRESET')) {
        throw e;
      } else {
        trace(`${url} Connection reset, retrying...`);
      }
    }
  }
}

(async () => {
  let program = new commander.Command();
  program
    .option('-q, --queue <queue>', '`jobs` or `jobs-success`', 'jobs-success')
    .option('-s, --no-state', 'only generate a jobs report');
  program.parse(process.argv);

  if (!program.noState) {
    console.log('state');
    // const shares = await getState();
    // createUserAccess(shares.tradingPartners);
    // createDocumentShares(shares.documents);
  }
  let jobs = await getShares({}, program.queue);
  createEventLog(jobs);
})();
