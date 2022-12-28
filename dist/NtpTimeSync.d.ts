import { NtpPacket } from "ntp-packet-parser";
import { NtpTimeResult } from "./NtpTimeResult";
import { RecursivePartial } from "./RecursivePartial";
export interface NtpTimeSyncConstructorOptions {
    servers: string[];
    sampleCount: number;
    replyTimeout: number;
    ntpDefaults: {
        port: number;
        version: number;
        tolerance: number;
        minPoll: number;
        maxPoll: number;
        maxDispersion: number;
        minDispersion: number;
        maxDistance: number;
        maxStratum: number;
        precision: number;
        referenceDate: Date;
    };
}
export interface NtpTimeSyncOptions extends Omit<NtpTimeSyncConstructorOptions, "servers"> {
    servers: ReadonlyArray<{
        host: string;
        port: number;
    }>;
}
export declare const NtpTimeSyncDefaultOptions: {
    servers: string[];
    sampleCount: number;
    replyTimeout: number;
    ntpDefaults: {
        port: number;
        version: number;
        tolerance: number;
        minPoll: number;
        maxPoll: number;
        maxDispersion: number;
        minDispersion: number;
        maxDistance: number;
        maxStratum: number;
        precision: number;
        referenceDate: Date;
    };
};
interface NtpReceivedPacket extends Partial<NtpPacket> {
    destinationTimestamp: Date;
}
export declare class NtpTimeSync {
    private options;
    private samples;
    constructor(options?: RecursivePartial<NtpTimeSyncConstructorOptions>);
    private recursiveResolveOptions;
    /**
     * Returns a singleton
     */
    static getInstance(options?: RecursivePartial<NtpTimeSyncConstructorOptions>): NtpTimeSync;
    private collectSamples;
    /**
     * @param {boolean} force Force NTP update
     */
    getTime(force?: boolean): Promise<NtpTimeResult>;
    /**
     * Will return the correct timestamp when function was called
     */
    now(force?: boolean): Promise<Date>;
    private static pad;
    /**
     * @param {Integer} leapIndicator, defaults to 3 (unsynchronized)
     * @param {Integer} ntpVersion, defaults to `options.ntpDefaults.version`
     * @param {Integer} mode, defaults to 3 (client)
     * @return {Buffer}
     */
    private createPacket;
    private static cleanup;
    getNetworkTime(server: string, port?: number): Promise<NtpReceivedPacket>;
    /**
     * Test if response is acceptable for synchronization
     */
    private acceptResponse;
    /**
     * Average for a list of numbers
     */
    private static avg;
    /**
     * Standard deviation for a list of numbers
     */
    private static stdDev;
}
export {};
