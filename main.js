const https = require('https')
const crypto = require('crypto')
const fs = require('fs')
const { URLSearchParams } = require('url')
let keys = JSON.parse(fs.readFileSync('./oms/keys.json'))
const API_KEY = keys['BinanceGlobalTest']['API_KEY']
const SECRET_KEY = keys['BinanceGlobalTest']['SECRET_KEY']
const hmac = crypto.createHmac('sha256', SECRET_KEY)

function signature(query_string) {
    return crypto
        .createHmac('sha256', SECRET_KEY)
        .update(query_string)
        .digest('hex');
}

function rest_query(payload, callback) {
    var params = payload.params ?? {}

    if (payload.signature ?? 0) {
        params.timestamp = Date.now()
        params.signature = signature(new URLSearchParams(params).toString())
        // console.log(params)
    }

    var options = {
        hostname: 'testnet.binance.vision',
        port: 443,
        headers: {
            'Content-type': 'application/x-www-form-urlencoded',
            'X-MBX-APIKEY': API_KEY
        }
    }
    if (payload.method == 'GET') {
        query_string = new URLSearchParams(params).toString()
        options.method = 'GET',
        options.path = payload.path + ((query_string == '') ? '' : `?${query_string}`)
    } else {
        query_string = new URLSearchParams(params).toString()
        options.method = payload.method
        options.path = payload.path + ((query_string == '') ? '' : `?${query_string}`)
    }
    // console.log(options)
    const req = https.request(options, res => {
        console.log(`statusCode: ${res.statusCode}`);
    
        let data = '';
        res.on('data', (chunk) => {
            data = data + chunk.toString();
            // console.log(chunk)
        });
      
        res.on('end', () => {
            let body = JSON.parse(data);
            callback(body);
        });
    })
    req.on('error', (error) => {
        console.error(error)
      })
    // if (payload.method != 'GET') {
    //     // console.log(JSON.stringify(params))
    //     req.write(JSON.stringify(params))
    // }
    req.end() 

}



// var payload = {
//     path: '/api/v3/account',
//     method: 'GET',
//     signature: true,
// }

// var payload = {
//     path: '/api/v3/openOrders',
//     method: 'GET',
//     params: {
//         symbol: 'BTCUSDT'
//     },
//     signature: true,
// }

var payload = {
    path: '/api/v3/order',
    method: 'POST',
    params: {
        symbol: 'BTCUSDT',
        side: 'BUY',
        type: 'LIMIT',
        quantity: 1.0,
        price: 2000.00,
        timeInForce: 'GTC'
    },
    signature: true,
}

rest_query(payload, res =>{
    console.log(res)
})
