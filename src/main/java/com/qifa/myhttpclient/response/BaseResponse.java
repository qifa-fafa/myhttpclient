package com.qifa.myhttpclient.response;

/**
 * 响应基类。第三方 API 的响应可以继承此类并添加具体数据字段。
 */
public class BaseResponse {
    // HTTP 或 业务码（取决于你希望使用的语义）
    private int code;
    // 错误/提示信息
    private String message;

    public BaseResponse() {}

    public BaseResponse(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public boolean isSuccess() {
        // 默认 2xx 认为成功；子类可自定义逻辑
        return code >= 200 && code < 300;
    }

    public int getCode() { return code; }
    public void setCode(int code) { this.code = code; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
