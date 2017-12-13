package volt.com.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import volt.com.config.Config;

public class ExecuteQuery {
	final static String proc = "GetDetailedCampaignBalance";
	FileWriter fw = null;
	BufferedWriter bw = null;

	public ExecuteQuery() {
		try {
			fw = new FileWriter("loadResponse_" + Config.logName + ".log", true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);
	}

	// native implementation of GetDetailedCampaignBalance procedure
	public void getDetailedCampaign(int counter) {
		ClientResponse clientResponse = null;
		ClientConfig config = new ClientConfig();
		config.enableAutoTune();
		Client client = VoltToolClientUtil.getClient();
		
		try {			
			System.out.println(counter + "- Executing SP ");
			long startTime = System.currentTimeMillis();
			double i = Math.random();
			int qureyIndex = (int) (i * 3);
			this.bw.write(counter + "- Executing volt SP GetDetailedCampaignBalance \n");
			if (qureyIndex == 0)
				clientResponse = client.callProcedure(proc, 206877, 1914093, 20150629);
			else if (qureyIndex == 1)
				clientResponse = client.callProcedure(proc, 110324, 1913978, 20150721);
			else
				clientResponse = client.callProcedure(proc, 200280, 1822947, 20150430);
			
			VoltTable[] voltdbResultSet = clientResponse.getResults();
			long endTime = System.currentTimeMillis();
			bw.write(counter + "- Result fetched in " + (endTime - startTime) + "ms\n\n\n\n");
			System.out.println("Result fetched \n");
			
			if (voltdbResultSet != null && voltdbResultSet.length > 0) {
				VoltTable voltTable = voltdbResultSet[0];
				voltTable.advanceRow();
				
				bw.write(counter + "- Result: " + "AdvertiserId: " + voltTable.get(0, VoltType.INTEGER) + 
						" CampaignId: " + voltTable.get(1, VoltType.INTEGER) + " BalanceDate: " + voltTable.get(2, VoltType.INTEGER) + 
						" BalanceDate: " + voltTable.get(2, VoltType.INTEGER) + 
						" CampaignRunningDailySpend " + voltTable.getDecimalAsBigDecimal(3) + 
						" CampaignRunningDailyPushSpend " + voltTable.getDecimalAsBigDecimal(4) + 
						" AdvertiserMaxDailyBudget " + voltTable.getDecimalAsBigDecimal(5) + 
						" CampaignMaxDailyBudget " + voltTable.getDecimalAsBigDecimal(6) + 
						" TotalCountDailyPush " + voltTable.get(7, VoltType.INTEGER) + 
						" TotalCountDailyClick " + voltTable.get(8, VoltType.INTEGER) + 
						" AdvertiserRunningDailySpend " + voltTable.getDecimalAsBigDecimal(9) + 
						" AdvertiserRunningDailyPushSpend " + voltTable.getDecimalAsBigDecimal(10) + 
						" AccountLedgerCredits " + voltTable.getDecimalAsBigDecimal(11) + 
						" AccountLedgerDebits " + voltTable.getDecimalAsBigDecimal(12) + 
						" AdvertiserAccountBalance " + voltTable.getDecimalAsBigDecimal(13) + "\n\n\n\n");
				
//				System.out.print("AdvertiserId: " + voltTable.get(0, VoltType.INTEGER));
//				System.out.print(" CampaignId: " + voltTable.get(1, VoltType.INTEGER));
//				System.out.print(" BalanceDate: " + voltTable.get(2, VoltType.INTEGER));
//				System.out.print(" CampaignRunningDailySpend " + voltTable.getDecimalAsBigDecimal(3));
//				System.out.print(" CampaignRunningDailyPushSpend " + voltTable.getDecimalAsBigDecimal(4));
//				System.out.print(" AdvertiserMaxDailyBudget " + voltTable.getDecimalAsBigDecimal(5));
//				System.out.print(" CampaignMaxDailyBudget " + voltTable.getDecimalAsBigDecimal(6));
//				System.out.print(" TotalCountDailyPush " + voltTable.get(7, VoltType.INTEGER));
//				System.out.print(" TotalCountDailyClick " + voltTable.get(8, VoltType.INTEGER));
//				System.out.print(" AdvertiserRunningDailySpend " + voltTable.getDecimalAsBigDecimal(9));
//				System.out.print(" AdvertiserRunningDailyPushSpend " + voltTable.getDecimalAsBigDecimal(10));
//				System.out.print(" AccountLedgerCredits " + voltTable.getDecimalAsBigDecimal(11));
//				System.out.print(" AccountLedgerDebits " + voltTable.getDecimalAsBigDecimal(12));
//				System.out.print(" AdvertiserAccountBalance " + voltTable.getDecimalAsBigDecimal(13));
			}

			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{

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

	}
}
