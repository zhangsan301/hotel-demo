package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;


@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private IHotelService hotelService;

    private RestHighLevelClient client;

    //新增文档
    @Test
    void testAddDocument() throws IOException {
        // 1.查询数据库hotel数据
        Hotel hotel = hotelService.getById(61083L);
        // 2.转换为HotelDoc
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 3.转JSON
        String json = JSON.toJSONString(hotelDoc);

        // 1.准备Request
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 2.准备请求参数DSL，其实就是文档的JSON字符串
        request.source(json, XContentType.JSON);
        // 3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }

    //获取文档
    @Test
    void testGetDocumentById() throws IOException {
        //1.准备Request对象
        GetRequest request = new GetRequest("hotel", "61083");
        //2.发送请求,得到响应
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //3.解析响应结果
        String json = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    //更新文档
    @Test
    void testUpdateDocument() throws IOException {
        //1.创建request对象
        UpdateRequest request = new UpdateRequest("hotel","61083");
        //2.准备参数,每2个参数为一对 key value
        request.doc(
            "price","952",
            "starName","四钻"
        );
        //3.发送请求
        client.update(request,RequestOptions.DEFAULT);
    }

    //删除文档
    @Test
    void testDeleteDocument() throws IOException {
        //1.准备request请求
        DeleteRequest request = new DeleteRequest("hotel","61083");
        //2.发送请求
        client.delete(request,RequestOptions.DEFAULT);
    }

    //批量导入
    @Test
    void testBulkRequest() throws IOException {
        List<Hotel> hotels = hotelService.list();

        //1.准备Request请求
          BulkRequest request= new BulkRequest();
        //2.准备参数,添加多个新增的Request
        for(Hotel hotel:hotels){
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //创建新增文档的Request对象
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        //3.发送请求
        client.bulk(request,RequestOptions.DEFAULT);
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
