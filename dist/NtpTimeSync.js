"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.NtpTimeSync = exports.NtpTimeSyncDefaultOptions = void 0;
var dgram = require("dgram");
var ntp_packet_parser_1 = require("ntp-packet-parser");
var singleton;
var lastPoll;
var lastResult;
exports.NtpTimeSyncDefaultOptions = {
    // list of NTP time servers, optionally including a port (defaults to options.ntpDefaults.port = 123)
    servers: ["0.pool.ntp.org", "1.pool.ntp.org", "2.pool.ntp.org", "3.pool.ntp.org"],
    // required amount of valid samples
    sampleCount: 8,
    // amount of time in milliseconds to wait for an NTP response
    replyTimeout: 3000,
    // defaults as of RFC5905
    ntpDefaults: {
        port: 123,
        version: 4,
        tolerance: 15e-6,
        minPoll: 4,
        maxPoll: 17,
        maxDispersion: 16,
        minDispersion: 0.005,
        maxDistance: 1,
        maxStratum: 16,
        precision: -18,
        referenceDate: new Date("Jan 01 1900 GMT"),
    },
};
var NtpTimeSync = /** @class */ (function () {
    function NtpTimeSync(options) {
        if (options === void 0) { options = {}; }
        this.samples = [];
        var serverConfig = options.servers || exports.NtpTimeSyncDefaultOptions.servers;
        var mergedConfig = this.recursiveResolveOptions(options, exports.NtpTimeSyncDefaultOptions);
        this.options = __assign(__assign({}, mergedConfig), { servers: serverConfig.map(function (server) {
                return {
                    host: server.split(":", 2)[0],
                    port: Number(server.split(":", 2)[1]) || mergedConfig.ntpDefaults.port,
                };
            }) });
    }
    NtpTimeSync.prototype.recursiveResolveOptions = function (options, defaults) {
        var _this = this;
        var mergedConfig = Object.entries(defaults).map(function (_a) {
            var key = _a[0], value = _a[1];
            // option was not defined in input
            if (!(key in options)) {
                return [key, value];
            }
            // option is invalid
            if (!(key in defaults)) {
                throw new Error("Invalid option: ".concat(key));
            }
            if (Array.isArray(options[key])) {
                return [key, options[key]];
            }
            var isObject = typeof options[key] === "object" && options[key] !== null && !(options[key] instanceof Date);
            if (isObject) {
                return [key, _this.recursiveResolveOptions(options[key], defaults[key])];
            }
            return [key, options[key]];
        });
        return Object.fromEntries(mergedConfig);
    };
    /**
     * Returns a singleton
     */
    NtpTimeSync.getInstance = function (options) {
        if (options === void 0) { options = {}; }
        if (!singleton) {
            singleton = new NtpTimeSync(options);
        }
        return singleton;
    };
    NtpTimeSync.prototype.collectSamples = function (numSamples) {
        return __awaiter(this, void 0, void 0, function () {
            var ntpResults, retry, _loop_1, this_1, samples;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        ntpResults = [];
                        retry = 0;
                        _loop_1 = function () {
                            var timePromises, _b, _c;
                            return __generator(this, function (_d) {
                                switch (_d.label) {
                                    case 0:
                                        timePromises = [];
                                        this_1.options.servers.forEach(function (server) {
                                            timePromises.push(_this.getNetworkTime(server.host, server.port).then(function (data) {
                                                _this.acceptResponse(data);
                                                return data;
                                            }));
                                        });
                                        _c = (_b = ntpResults)
                                            .concat;
                                        return [4 /*yield*/, Promise.all(timePromises.map(function (p) { return p.catch(function (e) { return e; }); }))];
                                    case 1:
                                        // wait for NTP responses to arrive
                                        ntpResults = _c.apply(_b, [_d.sent()])
                                            .filter(function (result) {
                                            return !(result instanceof Error);
                                        });
                                        if (ntpResults.length === 0) {
                                            retry++;
                                        }
                                        return [2 /*return*/];
                                }
                            });
                        };
                        this_1 = this;
                        _a.label = 1;
                    case 1: return [5 /*yield**/, _loop_1()];
                    case 2:
                        _a.sent();
                        _a.label = 3;
                    case 3:
                        if (ntpResults.length < numSamples && retry < 3) return [3 /*break*/, 1];
                        _a.label = 4;
                    case 4:
                        if (ntpResults.length === 0) {
                            throw new Error("Connection error: Unable to get any NTP response after " + retry + " retries");
                        }
                        samples = [];
                        ntpResults.forEach(function (data) {
                            var offsetSign = data.transmitTimestamp.getTime() > data.destinationTimestamp.getTime() ? 1 : -1;
                            var offset = ((Math.abs(data.receiveTimestamp.getTime() - data.originTimestamp.getTime()) +
                                Math.abs(data.transmitTimestamp.getTime() - data.destinationTimestamp.getTime())) /
                                2) *
                                offsetSign;
                            var delay = Math.max(data.destinationTimestamp.getTime() -
                                data.originTimestamp.getTime() -
                                (data.receiveTimestamp.getTime() - data.transmitTimestamp.getTime()), Math.pow(2, _this.options.ntpDefaults.precision));
                            var dispersion = Math.pow(2, data.precision) +
                                Math.pow(2, _this.options.ntpDefaults.precision) +
                                _this.options.ntpDefaults.tolerance * (data.destinationTimestamp.getTime() - data.originTimestamp.getTime());
                            samples.push({
                                data: data,
                                offset: offset,
                                delay: delay,
                                dispersion: dispersion,
                            });
                        });
                        // sort samples by ascending delay
                        samples.sort(function (a, b) {
                            return a.delay - b.delay;
                        });
                        // restrict to best n samples
                        return [2 /*return*/, samples.slice(0, numSamples)];
                }
            });
        });
    };
    /**
     * @param {boolean} force Force NTP update
     */
    NtpTimeSync.prototype.getTime = function (force) {
        if (force === void 0) { force = false; }
        return __awaiter(this, void 0, void 0, function () {
            var date_1, _a, offset, precision, date;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        if (!force && lastPoll && Date.now() - lastPoll < Math.pow(2, this.options.ntpDefaults.minPoll) * 1000) {
                            date_1 = new Date();
                            date_1.setUTCMilliseconds(date_1.getUTCMilliseconds() + lastResult.offset);
                            return [2 /*return*/, {
                                    now: date_1,
                                    offset: lastResult.offset,
                                    precision: lastResult.precision,
                                }];
                        }
                        // update time samples
                        _a = this;
                        return [4 /*yield*/, this.collectSamples(this.options.sampleCount)];
                    case 1:
                        // update time samples
                        _a.samples = _b.sent();
                        offset = this.samples.reduce(function (acc, item) {
                            return acc + item.offset;
                        }, 0) / this.samples.length;
                        precision = NtpTimeSync.stdDev(this.samples.map(function (sample) { return sample.offset; }));
                        lastResult = {
                            offset: offset,
                            precision: precision,
                        };
                        lastPoll = Date.now();
                        date = new Date();
                        date.setUTCMilliseconds(date.getUTCMilliseconds() + offset);
                        return [2 /*return*/, {
                                now: date,
                                offset: offset,
                                precision: precision,
                            }];
                }
            });
        });
    };
    /**
     * Will return the correct timestamp when function was called
     */
    NtpTimeSync.prototype.now = function (force) {
        if (force === void 0) { force = false; }
        return __awaiter(this, void 0, void 0, function () {
            var now, result;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        now = new Date();
                        return [4 /*yield*/, this.getTime(force)];
                    case 1:
                        result = _a.sent();
                        now.setUTCMilliseconds(now.getUTCMilliseconds() + result.offset);
                        return [2 /*return*/, now];
                }
            });
        });
    };
    NtpTimeSync.pad = function (string, length, char, side) {
        if (char === void 0) { char = "0"; }
        if (side === void 0) { side = "left"; }
        if (side === "left") {
            return char.repeat(length).substring(0, length - string.length) + string;
        }
        return string + char.repeat(length).substring(0, length - string.length);
    };
    /**
     * @param {Integer} leapIndicator, defaults to 3 (unsynchronized)
     * @param {Integer} ntpVersion, defaults to `options.ntpDefaults.version`
     * @param {Integer} mode, defaults to 3 (client)
     * @return {Buffer}
     */
    NtpTimeSync.prototype.createPacket = function (leapIndicator, ntpVersion, mode) {
        if (leapIndicator === void 0) { leapIndicator = 3; }
        if (ntpVersion === void 0) { ntpVersion = null; }
        if (mode === void 0) { mode = 3; }
        ntpVersion = ntpVersion || this.options.ntpDefaults.version;
        // generate NTP packet
        var ntpData = new Array(48).fill(0);
        ntpData[0] =
            // Leap indicator (= 3, unsynchronized)
            NtpTimeSync.pad((leapIndicator >>> 0).toString(2), 2) +
                // NTP version (= 4)
                NtpTimeSync.pad((ntpVersion >>> 0).toString(2), 3) +
                // client mode (= 3)
                NtpTimeSync.pad((mode >>> 0).toString(2), 3);
        ntpData[0] = parseInt(ntpData[0], 2);
        // origin timestamp
        var baseTime = new Date().getTime() - this.options.ntpDefaults.referenceDate.getTime();
        var seconds = baseTime / 1000;
        var ntpTimestamp = (seconds * Math.pow(2, 32)).toString(2);
        ntpTimestamp = NtpTimeSync.pad(ntpTimestamp, 64);
        // origin timestamp
        ntpData[24] = parseInt(ntpTimestamp.substr(0, 8), 2);
        ntpData[25] = parseInt(ntpTimestamp.substr(8, 8), 2);
        ntpData[26] = parseInt(ntpTimestamp.substr(16, 8), 2);
        ntpData[27] = parseInt(ntpTimestamp.substr(24, 8), 2);
        ntpData[28] = parseInt(ntpTimestamp.substr(32, 8), 2);
        ntpData[29] = parseInt(ntpTimestamp.substr(40, 8), 2);
        ntpData[30] = parseInt(ntpTimestamp.substr(48, 8), 2);
        ntpData[31] = parseInt(ntpTimestamp.substr(56, 8), 2);
        // transmit timestamp
        ntpData[40] = parseInt(ntpTimestamp.substr(0, 8), 2);
        ntpData[41] = parseInt(ntpTimestamp.substr(8, 8), 2);
        ntpData[42] = parseInt(ntpTimestamp.substr(16, 8), 2);
        ntpData[43] = parseInt(ntpTimestamp.substr(24, 8), 2);
        ntpData[44] = parseInt(ntpTimestamp.substr(32, 8), 2);
        ntpData[45] = parseInt(ntpTimestamp.substr(40, 8), 2);
        ntpData[46] = parseInt(ntpTimestamp.substr(48, 8), 2);
        ntpData[47] = parseInt(ntpTimestamp.substr(56, 8), 2);
        return Buffer.from(ntpData);
    };
    NtpTimeSync.cleanup = function (client) {
        try {
            client.close();
        }
        catch (e) {
            // ignore, as we just want to cleanup
        }
    };
    NtpTimeSync.prototype.getNetworkTime = function (server, port) {
        var _this = this;
        if (port === void 0) { port = 123; }
        return new Promise(function (resolve, reject) {
            var client = dgram.createSocket("udp4");
            var hasFinished = false;
            var errorCallback = function (err) {
                if (timeoutHandler) {
                    clearTimeout(timeoutHandler);
                    timeoutHandler = null;
                }
                if (hasFinished) {
                    return;
                }
                NtpTimeSync.cleanup(client);
                hasFinished = true;
                reject(err);
            };
            client.on("error", function (err) { return errorCallback; });
            // setup timeout
            var timeoutHandler = setTimeout(function () {
                errorCallback(new Error("Timeout waiting for NTP response."));
            }, _this.options.replyTimeout);
            client.send(_this.createPacket(), port, server, function (err) {
                if (hasFinished) {
                    return;
                }
                if (err) {
                    errorCallback(err);
                    return;
                }
                client.once("message", function (msg) {
                    if (hasFinished) {
                        return;
                    }
                    clearTimeout(timeoutHandler);
                    timeoutHandler = null;
                    client.close();
                    var result = __assign(__assign({}, ntp_packet_parser_1.NtpPacketParser.parse(msg)), { destinationTimestamp: new Date() });
                    hasFinished = true;
                    resolve(result);
                });
            });
        });
    };
    /**
     * Test if response is acceptable for synchronization
     */
    NtpTimeSync.prototype.acceptResponse = function (data) {
        /*
         * Format error
         */
        if (data.version > this.options.ntpDefaults.version) {
            throw new Error("Format error: Expected version " + this.options.ntpDefaults.version + ", got " + data.version);
        }
        /*
         * A stratum error occurs if (1) the server has never been
         * synchronized, (2) the server stratum is invalid.
         */
        if (data.leapIndicator === 3 || data.stratum >= this.options.ntpDefaults.maxStratum) {
            throw new Error("Stratum error: Remote clock is unsynchronized");
        }
        /*
         * Verify valid root distance.
         */
        var rootDelay = (data.rootDelay.getTime() - this.options.ntpDefaults.referenceDate.getTime()) / 1000;
        var rootDispersion = (data.rootDispersion.getTime() - this.options.ntpDefaults.referenceDate.getTime()) / 1000;
        if (rootDelay / 2 + rootDispersion >= this.options.ntpDefaults.maxDispersion) {
            throw new Error("Distance error: Root distance too large");
        }
        /*
         * Verify origin timestamp
         */
        if (data.originTimestamp.getTime() > new Date().getTime()) {
            throw new Error("Format error: Origin timestamp is from the future");
        }
    };
    /**
     * Average for a list of numbers
     */
    NtpTimeSync.avg = function (values) {
        var sum = values.reduce(function (sum, value) {
            return sum + value;
        }, 0);
        return sum / values.length;
    };
    /**
     * Standard deviation for a list of numbers
     */
    NtpTimeSync.stdDev = function (values) {
        var avg = this.avg(values);
        var squareDiffs = values.map(function (value) {
            var diff = value - avg;
            return diff * diff;
        });
        return Math.sqrt(this.avg(squareDiffs));
    };
    return NtpTimeSync;
}());
exports.NtpTimeSync = NtpTimeSync;
