import dotenv from "dotenv";
dotenv.config();

export default {
  ELASTICSEARCH_HOST: process.env.ELASTICSEARCH_HOST,
  INDEX_NAME: process.env.INDEX_NAME,
  ELASTIC_USERNAME: process.env.ELASTIC_USERNAME,
  ELASTIC_PASSWORD: process.env.ELASTIC_PASSWORD,
  REQUEST_HEADERS: {
    "User-Agent": process.env.USER_AGENT,
  },
};
