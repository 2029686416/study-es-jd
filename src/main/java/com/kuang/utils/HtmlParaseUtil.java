package com.kuang.utils;

import com.kuang.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

@Component
public class HtmlParaseUtil {
    public static void main(String[] args) throws IOException {

        new HtmlParaseUtil().paraseJD("java").forEach(System.out::println);
    }

    public ArrayList<Content> paraseJD(String keywords) throws IOException {
        //获取请求  https://search.jd.com/Search?keyword=java
        //前提，需要联网，ajax不能获取到！模拟浏览器！
        String url = "https://search.jd.com/Search?keyword="+keywords;
        //解析网页(Jsoup返回Document就是浏览器Document对象)
        Document document = Jsoup.parse(new URL(url), 30000);
        //所有在js中使用的方法，这都能用
        Element element = document.getElementById("J_goodsList");
//        System.out.println(element);
        //获取使用的li标签
        Elements elements = element.getElementsByTag("li");
        //获取元素中的内容，这里每个el，就是一个li
        ArrayList<Content> goodsList = new ArrayList<>();
        for (Element el : elements) {
            //关于图片，特别多的网站，所有的图片都是延迟加载的  source-data-lazy-img
//            String img = el.getElementsByTag("img").eq(0).attr("source-data-lazy-img");
            String img = el.getElementsByTag("img").eq(0).attr("src");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();
//            System.out.println("===============");
            Content content = new Content();
            content.setImg(img);
            content.setPrice(price);
            content.setTitle(title);
            goodsList.add(content);
        }
        return goodsList;

    }
}
