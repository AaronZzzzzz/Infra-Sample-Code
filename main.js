const child_process = require('child_process')
const SortedMap = require('collections/sorted-map')

class HashQueue {

    constructor(maxlen=1000) { 
        this.q = new Array();
        this.set = new Set();
        this.count = 0;
        this.maxlen = maxlen
    }

    send( item ) { 
        this.q.push( item );
        this.set.add( item );
        this.count += 1
        if (this.count >= this.maxlen ) {
            this.set.delete(this.q.shift())
            this.count -= 1
        }
    }

    has( item ) {
        return this.set.has( item );
    }

}


class Orderbook {

    constructor() {
        this.bid = new SortedMap()
        this.ask = new SortedMap()
        this.recent_update = new HashQueue(500)
        this.init = false
    }

    load(payload) {
        for (var i=0, len=payload['asks'].length; i<len; i++){
            this.ask.set(Number(payload['asks'][i][0]), Number(payload['asks'][i][1]))
        }
        for (var i=0, len=payload['bids'].length; i<len; i++){
            this.bid.set(-Number(payload['bids'][i][0]), Number(payload['bids'][i][1]))
        }
        // this.bid.addEach(payload['bids'])
        // this.ask.addEach(payload['asks'])
        this.init = true
        console.log(this.bid.length)
        console.log(this.ask.length)
    }

    update(payload) {
        var level = Number(payload['changes'][0][1]);
        var size =  Number(payload['changes'][0][2]);
        if (payload['changes'][0][0] == 'sell') {

            if (size == 0) {
                this.ask.delete(level)
            }
            else {
                this.ask.set(level, size)
            }
            
        }
        else {
            if (size == 0) {
                this.bid.delete(-level)
            }
            else {
                this.bid.set(-level, size)
            }
        }
    }

    get_notional_level(cutoff) {

        var bid_cum = 0
        var ask_cum = 0
        var bid_price = 0
        var ask_price = 0

        for (var e of this.bid.store.slice(0, 200)) {
            bid_cum += e['value']
            bid_price = -e['key']
            if (bid_cum >= cutoff) {break;}
        }

        for (var e of this.ask.store.slice(0, 200)) {
            ask_cum += e['value']
            ask_price = e['key']
            if (ask_cum >= cutoff) {break;}
        }
        return [bid_price, ask_price]
    }

    get_n_level(n) {

        if (!this.init) {return []}
        var bid_notional_sum = 0
        var bid_volume_sum = 0
        var ask_notional_sum = 0
        var ask_volume_sum = 0

        for (var e of this.bid.store.slice(0, n)) {
            bid_notional_sum += -e['key'] * e['value']
            bid_volume_sum += e['value']
        }
        for (var e of this.ask.store.slice(0, n)) {
            ask_notional_sum += e['key'] * e['value']
            ask_volume_sum += e['value']
        }
        return [bid_notional_sum/bid_volume_sum, ask_notional_sum/ask_volume_sum]
        // return 1
    }

    get_touch() {

        if (!this.init) {return []}
        var best_bid = this.bid.store.slice(0, 1)[0]
        best_bid = [-best_bid['key'], best_bid['value']]

        var best_ask = this.ask.store.slice(0, 1)[0]
        best_ask = [best_ask['key'], best_ask['value']]

        return [best_bid, best_ask]
    }

}


let q = new HashQueue()
var num_child = 3
var child_handle = new Array()
var orderbook = new Orderbook()
for (var i=0; i < num_child; i++) {
    child_handle[i] = child_process.spawn('node', ['subprocess.js'], {stdio: ['inherit', 'inherit', 'inherit', 'ipc']})
    child_handle[i].on('message', message => {
        if (!q.has(message['json'])) {
            q.send( message['json'] )
            // console.log(message)
            
            message['json'] = JSON.parse(message['json'])
            // console.log( message['json']['type'])
            if (message['json']['type'] == 'snapshot') {
                orderbook.load(message['json'])
            }
            else if (message['json']['type'] == 'l2update') {
                orderbook.update(message['json'])
                console.log(orderbook.get_notional_level(10))
            }
            else {console.log(message['json'])}
            console.log(Date.now() - message['timestamp'])
            
        };
    });
}
