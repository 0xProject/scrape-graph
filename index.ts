import { parse } from 'json2csv';
import { request } from 'graphql-request';
import * as fs from 'fs';


// https://github.com/graphprotocol/uniswap-subgraph/blob/b2918504af5a43290314a6c719f92850add857d4/schema.graphql#L115
interface UniswapExchangeHistoryEvent {
    id: string;
    tokenSymbol: string;
    type: string;
    timestamp: number;
    ethLiquidity: string;
    tokenLiquidity: string;
    ethBalance: string;
    tokenBalance: string;
    combinedBalanceInEth: string;
    combinedBalanceInUSD: string;
    ROI: string;
    totalUniToken: string;
    tokenPriceUSD: string;
    price: string;
    tradeVolumeToken: string;
    tradeVolumeEth: string;
    feeInEth: string;
}

const GRAPH_UNISWAP_GRAPHQL_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/graphprotocol/uniswap';

const generateQuery = (fromTimestamp: number) => {

    const query = `{
        exchangeHistoricalDatas(where: {timestamp_gt:${fromTimestamp}, tokenSymbol:"MKR"}, orderBy:timestamp, orderDirection: asc, first:1000){
          id
          tokenSymbol
          type
          timestamp
          ethLiquidity
          tokenLiquidity
          ethBalance
          tokenBalance
          combinedBalanceInEth
          combinedBalanceInUSD
          ROI
          totalUniToken
          tokenPriceUSD
          price
          tradeVolumeToken
          tradeVolumeEth
          feeInEth
        }
      }`
    return query;
}

const logForData = (data: UniswapExchangeHistoryEvent[]) => {
    const length = data.length;
    const { firstDate, lastDate } = getDateInfo(data);
    console.log(`Found ${length} entries.`);
    console.log(`First date: ${firstDate}`);
    console.log(`Last date: ${lastDate}`);
}

interface DateInfo {
    firstDate: Date;
    lastDate: Date;
}

const getDateInfo = (data: UniswapExchangeHistoryEvent[]): DateInfo => {
    const firstDate = new Date(data[0].timestamp*1000);
    const lastDate = new Date(data[data.length - 1].timestamp*1000);
    return { firstDate, lastDate };
}

const getFileName = (data: UniswapExchangeHistoryEvent[]) => {
    const { firstDate, lastDate } = getDateInfo(data);
    return `${firstDate.toISOString()} - ${lastDate.toISOString()}`;
}

const toCSV = (data: UniswapExchangeHistoryEvent[]): string => {
    const fields = ['id', 'tokenSymbol', 'type', 'timestamp', 'ethLiquidity', 'tokenLiquidity', 'ethBalance', 'tokenBalance', 'totalUniToken', 'price', 'feeInEth', 'tokenPriceUSD'];
    const opts = { fields };
    return parse(data, opts);
}

const scrape = async () => {
    let timestamp = 0;
    let allData: UniswapExchangeHistoryEvent[] = [];
    while (true) {
        let query = generateQuery(timestamp);
        const resp = await request(GRAPH_UNISWAP_GRAPHQL_ENDPOINT, query);
        const data: UniswapExchangeHistoryEvent[] = (resp as any).exchangeHistoricalDatas;
        if (data.length === 0) {
            break;
        }
        allData = allData.concat(data);
        logForData(data);
        timestamp = data[data.length - 1].timestamp;
    }
    console.log('Final data:');
    logForData(allData);
    const fileName = getFileName(allData);
    fs.writeFileSync(`data/${fileName}.json`, JSON.stringify(allData));
    fs.writeFileSync(`data/${fileName}.csv`, toCSV(allData));
}

scrape();