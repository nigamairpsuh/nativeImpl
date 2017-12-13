package volt.com.config;

public class Config {
//	Config for use in local machine

	public static String username="test";
	public static String password="test";
	public static String address="localhost";
	public static int nettyPort=9090;
	public static int tarantoolPort=3301;
//	// spaceIds
	public static int balanceSpaceID = 512;
	public static int transactionsSpaceID = 513;
	public static int spendsSpaceID = 514;
	
	
//	public static int campaignDailySpendsID = 525;
//	public static int advertiserLedgerID = 526;
//	public static int advertiserCamapignMapID = 527;
//
//	// stored procedure parameters used for queries in local machine
	public static int advertiserId = 1;
	public static int campaignId = 2;
	public static int balanceDate = 5555;
	
//	index constants
	public static int indexIteratorEqual = 0;
//##########################################################################
	
//	Config for use in production node
//	public static String username="apdev";
//	public static String password="kXu5Eu2xmfC8DSE3";
//	public static String address="10.10.34.10";
//	public static int nettyPort=9090;
//	public static int tarantoolPort=3303;
//	
//	// stored procedure parameters used for queries in production node
//	public static int advertiserId = 228849;
//	public static int campaignId = 2118232;
//	public static int balanceDate = 20171121;
//	
	public static int campaignDailySpendsID = 514;
	public static int advertiserLedgerID = 513;
	public static int advertiserCamapignMapID = 515;
	public static String logName = "";
}
