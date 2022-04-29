'use strict';
const WebSocket = require('ws')
// const url = 'wss://stream.binance.com/stream?streams=!bookTicker/btcusdt@trade/ethusdt@trade/solusdt@trade/dogeusdt@trade/gmtusdt@trade/apeusdt@trade/busdusdt@trade/darusdt@trade/bnbusdt@trade/lunausdt@trade/usdcusdt@trade'
const url = 'wss://ws-feed.exchange.coinbase.com'
const connection = new WebSocket(url)

console.log('Process ' + process.pid + ' Started')

connection.onopen = () => {
    connection.send(JSON.stringify({
        type: "subscribe",
        product_ids: [
            // "ETH-USD",
            "BTC-USD"
        ],
        channels: ["level2"]
    }))
}

connection.onmessage = (e) => {
    process.send({
        timestamp: Date.now(),
        json: e.data
    })
    // process.send(e.data)
}
