package com.bigdataprocessing;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static org.jboss.netty.handler.codec.rtsp.RtspHeaders.Names.USER_AGENT;

/**
 * Event generator makes API calls to Zillow Zestimate API and retrieves the response for processing
 * Below is a sample call to the API for zpid 48749425:
 *  http://www.zillow.com/webservice/GetZestimate.htm?zws-id=<ZWSID>&zpid=48749425
 */
public class EventGenerator {

    // API Key
    private static final String ZWSID = "X1-ZWz18v1jjzoft7_7oq0o";

    public static void main(String[] args) {

        // Fixes list of property IDs
        List<String> zpidList = Arrays.asList("56054087", "56169551","57547818", "57643225", "57643369",
                                              "57653023", "2091839350", "56430138", "2091856893", "56430483",
                                              "62827098", "2093815041", "2092618282", "56480059", "56097669");

        // Make API calls to Zillow for Real Estate Data
        try{
            for(int i=0; i < zpidList.size(); i++) {
                String url = "http://www.zillow.com/webservice/GetZestimate.htm?zws-id=" + ZWSID + "&zpid=" + zpidList.get(i);
                URL obj = new URL(url);
                HttpURLConnection con = (HttpURLConnection) obj.openConnection();
                con.setRequestMethod("GET");
                con.setRequestProperty("User-Agent", USER_AGENT);
                con.getResponseCode();
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                System.out.println(response.toString());


                // Places the results of the API call into the flume source within a event*.txt file
                Writer writer = null;
                String path = "/home/mike/Documents/flume_source/";
                try {
                    writer = new BufferedWriter(new OutputStreamWriter(
                            new FileOutputStream(path+"event"+i+"_"+System.currentTimeMillis()+".txt"), "utf-8"));
                    writer.write(response.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {writer.close();} catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }

        } catch (MalformedURLException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }


    }
}
