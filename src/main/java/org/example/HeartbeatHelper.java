package org.example;


public class HeartbeatHelper {

    public static final byte[] HB = {0x4D, 0x53};
    public static final byte[] HBC = {0x4D, 0x53, 0x54, 0x4D}; // readiness for kafka 1.0 upgrade

    public static boolean isHeartBeat(byte[] bytes) {
        boolean result = false;
        if (bytes.length == HB.length) {
            for (int i = 0; i < HB.length; i++) {
                result = (bytes[i] == HB[i]);
                if (!result) {
                    break;
                }
            }
        } else if (bytes.length == HBC.length) {
            for (int i = 0; i < HBC.length; i++) {
                result = (bytes[i] == HBC[i]);
                if (!result) {
                    break;
                }
            }
        }
        return result;
    }
}