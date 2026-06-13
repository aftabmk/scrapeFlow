const { EXCHANGE_1,EXCHANGE_100 } = process.env;

class TradeIdBuilder {
    static EXCHANGE = {
        [EXCHANGE_1]: 0,
        [EXCHANGE_100]: 1,
    };

    static CONTRACT = {
        OPTION: 0,
        FUTURE: 1,
        EQUITY: 2,
    };

    static STATUS = {
        PENDING: 0,
        SUCCESS: 1,
        ERROR: 2,
    };

    static EXCHANGE_REVERSE = {
        0: EXCHANGE_1,
        1: EXCHANGE_100,
    };

    static CONTRACT_REVERSE = {
        0: 'OPTION',
        1: 'FUTURE',
        2: 'EQUITY',
    };

    static STATUS_REVERSE = {
        0: 'PENDING',
        1: 'SUCCESS',
        2: 'ERROR',
    };

    static getElapsedMinutes() {
        const marketOpen = new Date();

        // 9:15 AM market open time
        marketOpen.setHours(9, 15, 0, 0);
        return Math.max(0,Math.floor((Date.now() - marketOpen.getTime()) / 60_000));
    }

    static catchError(EXCHANGE,CONTRACT) {
        if (!(EXCHANGE in this.EXCHANGE)) {
            throw new Error(`Invalid exchange: ${EXCHANGE}`);
        }
        
        if (!(CONTRACT in this.CONTRACT)) {
            throw new Error(`Invalid contract: ${CONTRACT}`);
        }
    }
    static build(job) {
        const {EXCHANGE, CONTRACT, STATUS = 'PENDING'} = job;

        this.catchError(EXCHANGE,CONTRACT);
        const elapsedMinutes = this.getElapsedMinutes();

        let id = 0;

        // bits 0-1 => exchange
        id |= this.EXCHANGE[EXCHANGE];

        // bits 2-3 => contract
        id |= this.CONTRACT[CONTRACT] << 2;

        // bits 4-5 => status
        id |= this.STATUS[STATUS] << 4;

        // bits 6+ => minutes since 9 : 15 AM IST (MARKET OPEN)
        id |= elapsedMinutes << 6;

        return id;
    }

    static decode(id) {
        const exchange = id & 0b11;
        const contract = (id >> 2) & 0b11;
        const status = (id >> 4) & 0b11;
        const elapsedMinutes = id >> 6;

        return {
            exchange: this.EXCHANGE_REVERSE[exchange],
            contract: this.CONTRACT_REVERSE[contract],
            status: this.STATUS_REVERSE[status],
            elapsedMinutes,
        };
    }

    static updateStatus(id, status) {
        if (!(status in this.STATUS)) {
            throw new Error(`Invalid status: ${status}`);
        }

        // Clear bits 4-5
        id &= ~(0b11 << 4);

        // Set new status
        id |= this.STATUS[status] << 4;

        return id.toString(2);
    }

    static getStatus(id) {
        const status = (id >> 4) & 0b11;
        return this.STATUS_REVERSE[status];
    }
}

module.exports = TradeIdBuilder;