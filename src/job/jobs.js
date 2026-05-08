'use strict';

const { JobProducer } = require('./jobProducer');
const {
  EXCHANGE_1,   PAGE_URL_1,   API_URL_1,   API_URL_BUILDER_1, REFERER_1,
  EXCHANGE_2,   PAGE_URL_2,   API_URL_2,   API_URL_BUILDER_2, REFERER_2,
  EXCHANGE_3,   PAGE_URL_3,   API_URL_3,   API_URL_BUILDER_3, REFERER_3,
  EXCHANGE_100, PAGE_URL_100, API_URL_100, API_URL_BUILDER_100, REFERER_100,
  EXCHANGE_101, PAGE_URL_101, API_URL_101, API_URL_BUILDER_101, REFERER_101
} = require('../constant');


const Job_1 = Object.freeze({
  id:          'EXCHANGE_1: OPTION',
  exchange:    EXCHANGE_1,
  page_url:    PAGE_URL_1,
  api_url:     API_URL_1,
  refer_url :  REFERER_1,
  url_builder: API_URL_BUILDER_1,
});

const Job_2 = Object.freeze({
  id:          'EXCHANGE_1 : FUTURE',
  exchange:    EXCHANGE_2,
  page_url:    PAGE_URL_2,
  api_url:     API_URL_2,
  refer_url :  REFERER_2,
  url_builder: API_URL_BUILDER_2,
});

const Job_3 = Object.freeze({
  id:          'EXCHANGE_1 : EQUITY',
  exchange:    EXCHANGE_3,
  page_url:    PAGE_URL_3,
  api_url:     API_URL_3,
  refer_url :  REFERER_3,
  url_builder: API_URL_BUILDER_3,
});

const Job_4 = Object.freeze({
  id:          'EXCHANGE_2 : OPTION',
  exchange:    EXCHANGE_100,
  page_url:    PAGE_URL_100,
  api_url:     API_URL_100,
  refer_url :  REFERER_100,
  url_builder: API_URL_BUILDER_100,
});

const Job_5 = Object.freeze({
  id:          'EXCHANGE_2 : FUTURE',
  exchange:    EXCHANGE_101,
  page_url:    PAGE_URL_101,
  api_url:     API_URL_101,
  refer_url :  REFERER_101,
  url_builder: API_URL_BUILDER_101,
});



const JOB_LIST = [
  Job_1,
  Job_2,
  Job_3,
  Job_4,
  Job_5,
];


function produceJobs(producerId = 'prod-1') {
  const producer = new JobProducer(producerId);
  return producer.createJobs(JOB_LIST);
}

module.exports = { produceJobs, JOB_LIST };