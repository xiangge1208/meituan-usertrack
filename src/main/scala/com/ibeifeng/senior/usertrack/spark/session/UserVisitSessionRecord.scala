package com.ibeifeng.senior.usertrack.spark.session

import com.alibaba.fastjson.JSONObject

case class UserVisitSessionRecord(
                                               date: String,
                                               userId: Long,
                                               sessionId: String,
                                               pageId: Long,
                                               actionTime: String,
                                               searchKeyword: String,
                                               clickCategoryId: String,
                                               clickProductId: String,
                                               orderCategoryIds: String,
                                               orderProductIds: String,
                                               payCategoryIds: String,
                                               payProductIds: String,
                                               cityId: Int
                                             ) {
  /**
    * 转换成为JSONObject对象
    *
    * @return
    */
  def transform2JSONObject(): JSONObject = {
    val record = new JSONObject()
    record.put("date", date)
    record.put("user_id", userId)
    record.put("session_id", sessionId)
    record.put("page_id", pageId)
    record.put("action_time", actionTime)
    record.put("search_keyword", searchKeyword)
    record.put("click_category_id", clickCategoryId)
    record.put("click_product_id", clickProductId)
    record.put("order_category_ids", orderCategoryIds)
    record.put("order_product_ids", orderProductIds)
    record.put("pay_category_ids", payCategoryIds)
    record.put("pay_product_ids", payProductIds)
    record.put("city_id", cityId)

    record
  }
}