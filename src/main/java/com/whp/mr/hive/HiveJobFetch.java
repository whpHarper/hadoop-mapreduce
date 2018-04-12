package com.whp.mr.hive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.whp.mr.hive.connection.RMResourceFetcher;
import com.whp.mr.hive.model.MRJobsWrapper;
import com.whp.mr.hive.utils.Constants;
import com.whp.mr.hive.model.AppInfo;
import com.whp.mr.hive.model.MRJob;
import com.whp.mr.hive.utils.InputStreamUtils;
import com.whp.mr.hive.utils.URLConnectionUtils;
import com.whp.mr.hive.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.net.URLConnection;
import java.util.*;

/**
 * @author whp 18-4-8
 */
public class HiveJobFetch {
    private static final Logger LOG = LoggerFactory.getLogger(HiveJobFetch.class);
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
    private static final String XML_HTTP_HEADER = "Accept";
    private static final String XML_FORMAT = "application/xml";
    private static final int CONNECTION_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 10000;
    private static String historyUrl="hiveJobFetch";
//    private JobIdFilter jobFilter;
    public HiveJobFetch(String historyUrl){
        this.historyUrl=historyUrl;
    }
    private boolean fetchRunningConfig(AppInfo appInfo, List<MRJob> mrJobs) {
        InputStream is = null;
        for (MRJob mrJob : mrJobs) {
            String confURL = appInfo.getTrackingUrl() + Constants.MR_JOBS_URL + "/" + mrJob.getId() + "/" + Constants.MR_CONF_URL + "?" + Constants.ANONYMOUS_PARAMETER;
            try {
                final URLConnection connection = URLConnectionUtils.getConnection(confURL);
                connection.setRequestProperty(XML_HTTP_HEADER, XML_FORMAT);
                connection.setConnectTimeout(CONNECTION_TIMEOUT);
                connection.setReadTimeout(READ_TIMEOUT);
                is = connection.getInputStream();
                Map<String, String> hiveQueryLog = new HashMap<>();
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, Boolean.TRUE);
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document dt = db.parse(is);
                Element element = dt.getDocumentElement();
                NodeList propertyList = element.getElementsByTagName("property");
                int length = propertyList.getLength();
                for (int i = 0; i < length; i++) {
                    Node property = propertyList.item(i);
                    String key = property.getChildNodes().item(0).getTextContent();
                    String value = property.getChildNodes().item(1).getTextContent();
                    hiveQueryLog.put(key, value);
                }

//                if (hiveQueryLog.containsKey(Constants.HIVE_QUERY_STRING)) {
//                    collector.emit(new ValuesArray(appInfo.getUser(), mrJob.getId(), Constants.ResourceType.JOB_CONFIGURATION, hiveQueryLog), mrJob.getId());
//                }
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            } finally {
                Utils.closeInputStream(is);
            }
        }
        return true;
    }
    public void handleApps(List<AppInfo> apps, boolean isRunning) {
        List<MRJob> mrJobs = new ArrayList<>();
        //fetch job config
        if (isRunning) {
            for (AppInfo appInfo : apps) {
//                if (!jobFilter.accept(appInfo.getId())) {
//                    continue;
//                }

                String jobURL = appInfo.getTrackingUrl() + Constants.MR_JOBS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
                InputStream is = null;
                try {
                    is = InputStreamUtils.getInputStream(jobURL, null, Constants.CompressionType.NONE);
//                    LOG.info("fetch mr job from {}", jobURL);
                    mrJobs = OBJ_MAPPER.readValue(is, MRJobsWrapper.class).getJobs().getJob();
                } catch (Exception e) {
                    LOG.warn("fetch mr job from {} failed, {}", jobURL, e);
                    continue;
                } finally {
                    Utils.closeInputStream(is);
                }

                if (fetchRunningConfig(appInfo, mrJobs)) {
                    continue;
                }
            }
        }

        if (!isRunning) {
            for (AppInfo appInfo : apps) {
//                if (!jobFilter.accept(appInfo.getId())) {
//                    continue;
//                }
                MRJob mrJob = new MRJob();
                mrJob.setId(appInfo.getId().replace("application_", "job_"));
                mrJobs.add(mrJob);
                fetchFinishedConfig(appInfo, mrJobs);
                mrJobs.clear();
            }
        }
    }

    private boolean fetchFinishedConfig(AppInfo appInfo, List<MRJob> mrJobs) {
        InputStream is = null;
        for (MRJob mrJob : mrJobs) {
            String urlString = /*crawlConfig.endPointConfig.HSBasePath +*/historyUrl+"jobhistory/conf/" + mrJob.getId() + "?" + Constants.ANONYMOUS_PARAMETER;
            try {
                is = InputStreamUtils.getInputStream(urlString, null, Constants.CompressionType.NONE);
                final org.jsoup.nodes.Document doc = Jsoup.parse(is, "UTF-8", urlString);
                doc.outputSettings().prettyPrint(false);
                org.jsoup.select.Elements elements = doc.select("table[id=conf]").select("tbody").select("tr");
                Map<String, String> hiveQueryLog = new HashMap<>();
                Iterator<org.jsoup.nodes.Element> iter = elements.iterator();
                while (iter.hasNext()) {
                    org.jsoup.nodes.Element element = iter.next();
                    org.jsoup.select.Elements tds = element.children();
                    String key = tds.get(0).text();
                    String value = "";
                    org.jsoup.nodes.Element valueElement = tds.get(1);
                    if (Constants.HIVE_QUERY_STRING.equals(key)) {
                        for (org.jsoup.nodes.Node child : valueElement.childNodes()) {
                            if (child instanceof TextNode) {
                                TextNode valueTextNode = (TextNode) child;
                                value = valueTextNode.getWholeText();
                                value = StringUtils.strip(value);
                            }
                        }
                    } else {
                        value = valueElement.text();
                    }
                    hiveQueryLog.put(key, value);
                }
                if (hiveQueryLog.containsKey(Constants.HIVE_QUERY_STRING)) {
//                    collector.emit(new ValuesArray(appInfo.getUser(), mrJob.getId(), Constants.ResourceType.JOB_CONFIGURATION, hiveQueryLog), mrJob.getId());
                    String sql=hiveQueryLog.get(Constants.HIVE_QUERY_STRING);
                    Parser parser=new Parser();
                    HiveQLParserContent content = new HiveQLParserContent();
                    content = parser.run(hiveQueryLog.get(Constants.HIVE_QUERY_STRING));
                    System.out.println("======"+content.toString());
                }
            } catch (Exception e) {
//                e.printStackTrace();
                return false;
            } finally {
                Utils.closeInputStream(is);
            }
        }
        return true;
    }

    public static void main(String[] args){
        RMResourceFetcher rmResourceFetcher=new RMResourceFetcher("http://192.168.1.90:8088/",null);
        try {
            String lastFinishedTime = "1479244718794";
            List<AppInfo> appInfos=rmResourceFetcher.getResource(Constants.ResourceType.COMPLETE_MR_JOB,lastFinishedTime);
            HiveJobFetch hiveJobFetch=new HiveJobFetch("http://192.168.1.90:19888/");
            hiveJobFetch.handleApps(appInfos,false);

            System.out.println(appInfos.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

   /* public static void main(String args[]){
        AppInfo appInfo=new AppInfo();
        appInfo.setTrackingUrl("http://192.168.1.90:19888/");
        List<MRJob> mrJobs=new ArrayList<>();
        MRJob mrJob=new MRJob();
        mrJob.setId("job_1520568057322_0182");
        HiveJobFetch hiveJobFetch=new HiveJobFetch();
        mrJobs.add(mrJob);
        hiveJobFetch.fetchFinishedConfig(appInfo,mrJobs);
    }*/
}
