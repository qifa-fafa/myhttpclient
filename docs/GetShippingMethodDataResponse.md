

# GetShippingMethodDataResponse


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**addressValidationEnabled** | **Integer** | 是否支持地址校验 |  |
|**code** | **String** | 物流产品编码，大小写敏感，创建相关单据引用该代码时，请遵从系统提供的代码传入参数值 |  [optional] |
|**deliveryTimeList** | **List&lt;String&gt;** | 支持的到货时间段 |  [optional] |
|**distributorType** | **Integer** | 配送商类型 |  [optional] |
|**insuranceCurrency** | **String** | 投保金额的币种信息 |  [optional] |
|**insuranceRange** | **String** | 投保金额范围值 |  [optional] |
|**isInsurance** | **Integer** | 保险服务 |  [optional] |
|**isOoh** | **Integer** | 是否支持户外配送 0否 1是 |  |
|**isOptionalBoard** | **Integer** | 是否打板 0否 1是 |  |
|**isSignature** | **Integer** | 是否支持签名服务的增值服务 |  |
|**isSpecifyArrivalTime** | **Integer** | 是否指定到货时间 |  [optional] |
|**isTruck** | **Integer** | 是否卡派渠道 |  |
|**name** | **String** | 物流产品中文名称 |  [optional] |
|**nameEn** | **String** | 物流产品英文名称 |  [optional] |
|**smBusinessType** | **String** | 业务类型（旧的业务类型，不推荐对接） |  [optional] |
|**spCode** | **String** | 服务商代码 |  |
|**transportMode** | **Integer** | 运输方式 |  [optional] |
|**type** | **String** | 物流产品类型 |  |
|**warehouseCode** | **String** | 仓库代码 |  |



