package com.packtpub.storm.trident.model;

import java.io.Serializable;

/**
 * 疾病事件
 */
public class DiagnosisEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    // 维度
    public double lat;
    // 精度
    public double lng;
    public long time;

    // 疾病编码
    public String diagnosisCode;

    public DiagnosisEvent(double lat, double lng, long time, String diagnosisCode) {
        super();
        this.time = time;
        this.lat = lat;
        this.lng = lng;
        this.diagnosisCode = diagnosisCode;
    }
}
