package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.util.ArrayBuilders;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.http.HttpHost;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@SpringBootTest
public class HotelSearchTest {
    private RestHighLevelClient client;

    @Test
    void testMatchAll() throws IOException {
      //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
      //2.准备DSL
        request.source()
                .query(QueryBuilders.matchAllQuery());
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析数据
        handleResponse(response);
        System.out.println(response);
    }

    @Test
    void testMatch() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        request.source()
                .query(QueryBuilders.matchQuery("all","如家"));
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    @Test
    void testBool() throws IOException {
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备请求参数
         BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 2.1.must
        boolQuery.must(QueryBuilders.termQuery("city", "杭州"));
        // 2.2.filter
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));

        request.source().query(boolQuery);
        // 3.发送请求，得到响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.结果解析
        handleResponse(response);
    }

    @Test
    void testPageAndSort() throws IOException {
        //页码,每页大小
        int page = 2,size=5;
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备请求参数
        //2.1. query
        request.source().query(QueryBuilders.matchAllQuery());
        //2.2 排序sort
        request.source().sort("price", SortOrder.ASC);
        //2.3 分页from、size
        request.source().from((page - 1)*size).size(5);
        // 3.发送请求，得到响应
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.结果解析
        handleResponse(response);
    }

    @Test
    void testHighLight() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        //2.1 query
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        //2.2 高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析响应
        handleResponse(response);
    }

     @Test
    private void handleResponse(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        // 4.1.总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("总条数：" + total);
        // 4.2.获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3.遍历
        for (SearchHit hit : hits) {
            // 4.4.获取source
            String json = hit.getSourceAsString();
            // 4.5.反序列化，非高亮的
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // 4.6.处理高亮结果
            // 1)获取高亮map
            Map<String, HighlightField> map = hit.getHighlightFields();
            // 2）根据字段名，获取高亮结果
            HighlightField highlightField = map.get("name");
            // 3）获取高亮结果字符串数组中的第1个元素
            String hName = highlightField.getFragments()[0].toString();
            // 4）把高亮结果放到HotelDoc中
            hotelDoc.setName(hName);
            // 4.7.打印
            System.out.println(hotelDoc);
        }
    }







    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.214.134:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }



}
