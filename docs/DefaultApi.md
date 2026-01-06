# DefaultApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**publicOpenBaseDataGetShippingMethodPost**](DefaultApi.md#publicOpenBaseDataGetShippingMethodPost) | **POST** /public_open/base_data/get_shipping_method | 获取物流产品 |


<a id="publicOpenBaseDataGetShippingMethodPost"></a>
# **publicOpenBaseDataGetShippingMethodPost**
> GetShippingMethodResponse publicOpenBaseDataGetShippingMethodPost(appToken, appKey, getShippingMethodRequest)

获取物流产品



### Example
```java
// Import classes:
import org.dromara.threepart.logistics.goodcang.client.ApiClient;
import org.dromara.threepart.logistics.goodcang.client.ApiException;
import org.dromara.threepart.logistics.goodcang.client.Configuration;
import org.dromara.threepart.logistics.goodcang.client.models.*;
import org.dromara.threepart.logistics.goodcang.api.DefaultApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("http://localhost");

    DefaultApi apiInstance = new DefaultApi(defaultClient);
    String appToken = "{{app-token-uat}}"; // String | 
    String appKey = "{{app-key-uat}}"; // String | 
    GetShippingMethodRequest getShippingMethodRequest = new GetShippingMethodRequest(); // GetShippingMethodRequest | 
    try {
      GetShippingMethodResponse result = apiInstance.publicOpenBaseDataGetShippingMethodPost(appToken, appKey, getShippingMethodRequest);
      System.out.println(result);
    } catch (ApiException e) {
      System.err.println("Exception when calling DefaultApi#publicOpenBaseDataGetShippingMethodPost");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters

| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **appToken** | **String**|  | [optional] [default to {{app-token-uat}}] |
| **appKey** | **String**|  | [optional] [default to {{app-key-uat}}] |
| **getShippingMethodRequest** | [**GetShippingMethodRequest**](GetShippingMethodRequest.md)|  | [optional] |

### Return type

[**GetShippingMethodResponse**](GetShippingMethodResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** |  |  -  |

