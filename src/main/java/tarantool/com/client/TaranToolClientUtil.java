package tarantool.com.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;

import tarantool.com.config.Config;

public class TaranToolClientUtil {

	private static TarantoolClient client;

	static long totalResponseTime = 0; // keep adding execution time of each
										// request
	static int countofRequests = 0; // number of requests executed at any given
									// moment
	// averageResponseTime = totalResponseTime/countofRequests

	// queries executed inside GetDetailedCampaignBalance procedure
	static String campaignTransactions = "airpushdb.ViewList().model().vw_with_nfr_aggr_campaign_daily_transactions("
			+ Config.advertiserId + "," + Config.campaignId + "," + Config.balanceDate + ")";

	static String advertiserLedger = "airpushdb.ViewList().model().vw_aggr_advertiser_ledger_balance("
			+ Config.advertiserId + ")";

	static String advertiserDailySpends = "airpushdb.ViewList().model().vw_with_nfr_aggr_advertiser_daily_spends("
			+ Config.advertiserId + "," + Config.balanceDate + ")";

	static String advertiserCampaignMap = "airpushdb.TableList().Advertiser_campaign_map().model().select_by_advertiser_id_campaign_id("
			+ Config.advertiserId + "," + Config.campaignId + ")";

	static FileWriter fw = null;
	static BufferedWriter bw = null;
	static {
		try {
			fw = new FileWriter("loadResponse_" + Config.logName + ".log", true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);
	}

	// native implementation of GetDetailedCampaignBalance procedure
	public static Map getDetailedCampaign(int advertiserId, int campaingId, long balanceDate) {
		Map<String, Object> result = new HashMap<String, Object>(); 

		try {
			TarantoolClient client = getClient();

			bw.write("Executing native SP implementation\n");
			System.out.println("Executing Native SP implementation\n");

			long startTime = System.currentTimeMillis();
			
//			QUERY DATA FROM vw_with_nfr_aggr_campaign_daily_transactions
			long queryStartTime = startTime;
//			parameters for select function-
//			param1 : spaceId
//			param2 : index number
//			param3 : list of keys
//			param4 : offset
//			param5 : limit of records returned
//			param6 : iterator type
			List campaignSpendsData = client.syncOps().select(Config.campaignDailySpendsID, 2, Arrays.asList(advertiserId, campaingId, balanceDate),
					0, Integer.MAX_VALUE, Config.indexIteratorEqual);
			if (campaignSpendsData.size() == 0)
				return null;
			bw.write("Result fetched from CampaignDailyspends in " + (System.currentTimeMillis() - queryStartTime) + "ms\n");
			
			int TOT_CNT_DAILY_PUSH = 0;
			int TOT_CNT_DAILY_CLICK = 0;
			int CAMPAIGN_RUNNING_DAILY_SPEND = 0;
			int CAMPAIGN_RUNNING_DAILY_PUSH_SPEND = 0;
			
			for(int i=0; i<campaignSpendsData.size(); i++){
				ArrayList tuple = (ArrayList) campaignSpendsData.get(0); 
				TOT_CNT_DAILY_PUSH = TOT_CNT_DAILY_PUSH + (Integer)tuple.get(5) + (Integer)tuple.get(11);
				TOT_CNT_DAILY_CLICK = TOT_CNT_DAILY_CLICK + (Integer)tuple.get(6) + (Integer)tuple.get(12);
				CAMPAIGN_RUNNING_DAILY_SPEND = CAMPAIGN_RUNNING_DAILY_SPEND + (Integer)tuple.get(7) + (Integer)tuple.get(13);
				CAMPAIGN_RUNNING_DAILY_PUSH_SPEND = CAMPAIGN_RUNNING_DAILY_PUSH_SPEND + (Integer)tuple.get(8) + (Integer)tuple.get(14);				
			}
			result.put("TOT_CNT_DAILY_PUSH", TOT_CNT_DAILY_PUSH);
			result.put("TOT_CNT_DAILY_CLICK", TOT_CNT_DAILY_CLICK);
			result.put("CAMPAIGN_RUNNING_DAILY_SPEND", CAMPAIGN_RUNNING_DAILY_SPEND);
			result.put("CAMPAIGN_RUNNING_DAILY_PUSH_SPEND", CAMPAIGN_RUNNING_DAILY_PUSH_SPEND);
			
			
			
//			QUERY DATA FROM vw_aggr_advertiser_ledger_balance
			queryStartTime = System.currentTimeMillis();
//			primary index=0 (2nd argument)
			List advertiserLedgerData = client.syncOps().select(Config.advertiserLedgerID, 0, Arrays.asList(advertiserId),
					0, Integer.MAX_VALUE, Config.indexIteratorEqual);
			
			if (advertiserLedgerData.size() == 0)
				return null;
			bw.write("Result fetched from advertiserLedger in " + (System.currentTimeMillis() - queryStartTime) + "ms\n");
			
			double ACCOUNT_LEDGER_CREDITS = 0;
			double ACCOUNT_LEDGER_DEBITS = 0;
			
			for(int i=0; i<advertiserLedgerData.size(); i++){
				ArrayList tuple = (ArrayList) advertiserLedgerData.get(0); 
				ACCOUNT_LEDGER_CREDITS = ACCOUNT_LEDGER_CREDITS + (Double)tuple.get(1);
				ACCOUNT_LEDGER_DEBITS = ACCOUNT_LEDGER_DEBITS + (Double)tuple.get(2);						
			}
			result.put("ACCOUNT_LEDGER_CREDITS", ACCOUNT_LEDGER_CREDITS);
			result.put("ACCOUNT_LEDGER_DEBITS", ACCOUNT_LEDGER_DEBITS);
			
			
			
//			QUERY DATA FROM vw_with_nfr_aggr_advertiser_daily_spends
			queryStartTime = System.currentTimeMillis();
			List campaignDailySpendsData = client.syncOps().select(Config.campaignDailySpendsID, 3, Arrays.asList(advertiserId, balanceDate),
					0, Integer.MAX_VALUE, Config.indexIteratorEqual);
			if (campaignDailySpendsData.size() == 0)
				return null;
			bw.write("Result fetched from CampaignDailyspends in " + (System.currentTimeMillis() - queryStartTime) + "ms\n");
			
			int TOTAL_RUNNING_DAILY_SPEND = 0;
			int TOTAL_RUNNING_DAILY_PUSH_SPEND = 0;
			
			
			for(int i=0; i<campaignDailySpendsData.size(); i++){
				ArrayList tuple = (ArrayList) campaignDailySpendsData.get(0); 
				TOTAL_RUNNING_DAILY_SPEND = TOTAL_RUNNING_DAILY_SPEND + (Integer)tuple.get(7) + (Integer)tuple.get(13);		
				TOTAL_RUNNING_DAILY_PUSH_SPEND = TOTAL_RUNNING_DAILY_PUSH_SPEND + (Integer)tuple.get(8) + (Integer)tuple.get(14);
			}
			result.put("TOTAL_RUNNING_DAILY_SPEND", TOTAL_RUNNING_DAILY_SPEND);
			result.put("TOTAL_RUNNING_DAILY_PUSH_SPEND", TOTAL_RUNNING_DAILY_PUSH_SPEND);
			
			
			
//			QUERY DATA FROM vw_with_nfr_aggr_campaign_daily_transactions
			queryStartTime = System.currentTimeMillis();
			List advertiserCampaignMapData = client.syncOps().select(Config.advertiserCamapignMapID, 0, Arrays.asList(advertiserId, campaingId),
					0, Integer.MAX_VALUE, Config.indexIteratorEqual);
			if (advertiserCampaignMapData.size() == 0)
				return null;
			bw.write("Result fetched from AdvertiserCampaignMap in " + (System.currentTimeMillis() - queryStartTime) + "ms\n");
			
			double ADVERTISER_MAX_DAILY_BUDGET = 0;
			int CAMPAIGN_MAX_DAILY_BUDGET = 0;
			int IS_ADVERTISER_ENABLED = 0;
			int IS_CAMPAIGN_ENABLED = 0;
			

			for(int i=0; i<advertiserCampaignMapData.size(); i++){
				ArrayList tuple = (ArrayList) advertiserCampaignMapData.get(0); 
				ADVERTISER_MAX_DAILY_BUDGET = (Double)tuple.get(2);		
				CAMPAIGN_MAX_DAILY_BUDGET = (Integer)tuple.get(3);
				IS_ADVERTISER_ENABLED = (Integer)tuple.get(4);
				IS_CAMPAIGN_ENABLED = (Integer)tuple.get(5);
			}
			result.put("ADVERTISER_MAX_DAILY_BUDGET", ADVERTISER_MAX_DAILY_BUDGET);
			result.put("CAMPAIGN_MAX_DAILY_BUDGET", CAMPAIGN_MAX_DAILY_BUDGET);
			result.put("IS_ADVERTISER_ENABLED", IS_ADVERTISER_ENABLED);
			result.put("IS_CAMPAIGN_ENABLED", IS_CAMPAIGN_ENABLED);
			
			
			
			long endTime = System.currentTimeMillis();
			System.out.println("time took in executing Native SP implementation " + (endTime - startTime));
			bw.write("time took in executing Native SP implementation " + (endTime - startTime) + "\n");

			countofRequests++;
			totalResponseTime = totalResponseTime + (endTime - startTime);
			bw.write("Average Response Time till " + countofRequests + " requests is "
					+ (totalResponseTime / countofRequests));
			bw.write("\n**********************************\n\n\n");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		finally {
			try {
				// not closing the stream in order to use it to log other
				// requests
				if (bw != null)
					bw.flush();

				if (fw != null)
					fw.flush();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}
		return result;
	}

	public static void main(String[] args) {
		TarantoolClient client = getClient();
		getDetailedCampaign(Config.advertiserId, Config.campaignId, Config.balanceDate);
		// System.out.println(client.syncOps().call("airpushdb.ViewList().model().vw_with_nfr_aggr_campaign_daily_transactions",
		// 1 , 2, 5555));
		// System.out.println(client.syncOps().call("campaignDailyTransactions",
		// 1 , 2, 5555));

		// System.out.println(client.syncOps().eval(campaignTransactions, 1));
		// client.syncOps().eval(advertiserLedger, 1);
		// client.syncOps().eval(advertiserDailySpends, 1);
		// client.syncOps().eval(advertiserCampaignMap, 1);

	}

	static TarantoolClient getClient() {
		if (client != null)
			return client;

		TarantoolClientConfig config = new TarantoolClientConfig();
		config.username = Config.username;
		config.password = Config.password;
		config.initTimeoutMillis = 60000;

		SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
			public SocketChannel get(int retryNumber, Throwable lastError) {
				if (lastError != null) {
					lastError.printStackTrace(System.out);
				}
				try {
					return SocketChannel.open(new InetSocketAddress(Config.address, Config.tarantoolPort));
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}
		};
		return new TarantoolClientImpl(socketChannelProvider, config);
	}

}
