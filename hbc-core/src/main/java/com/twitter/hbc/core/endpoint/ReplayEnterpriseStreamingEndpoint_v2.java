package com.twitter.hbc.core.endpoint;

import com.google.common.base.Preconditions;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ReplayEnterpriseStreamingEndpoint_v2 extends EnterpriseStreamingEndpoint_v2{
    private static final String DATE_FMT_STR = "yyyyMMddHHmm";
    private static final String BASE_PATH = "/replay/%s/accounts/%s/publishers/%s/%s.json"; //product, account_name, publisher, stream_label

    private final Date fromDate;
    private final Date toDate;

    public ReplayEnterpriseStreamingEndpoint_v2(String account, String product, String label, Date fromDate, Date toDate) {
        super(account, product, label);
        this.fromDate = Preconditions.checkNotNull(fromDate);
        this.toDate = Preconditions.checkNotNull(toDate);
    }

    @Override
    public String getURI() {
        String uri = String.format(BASE_PATH, product.trim(), account.trim(), publisher.trim(), label.trim());

        addQueryParameter("fromDate", formatDate(this.fromDate));
        addQueryParameter("toDate", formatDate(this.toDate));

        return uri + "?" + generateParamString(queryParameters);
    }

    private String formatDate(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FMT_STR);
        return dateFormat.format(date);
    }
}
