/// <summary>
/// Imports a single file containing Twitter data from a Collective Intellect web feed
/// into a company's Social Media database.
/// </summary>
public void ImportSingleFileFromUrl(string sourcePath, string date, string number, bool archivingOn = true, string archivePath = null)
{
	using (WebClient client = new WebClient())
	{
		// Web request setup
		client.Credentials = ConnectionProvider.GetNetworkCredentials();
		client.Headers.Add("user-agent", WebClientUserAgent);

		// Format file name for request
		string downloadFileName = String.Format("{0}{1}_{2}.zip", _FILENAME_PREFIX, date, number);
		if (log.IsInfoEnabled) log.Info("Importing file " + downloadFileName + " from Collective Intellect...");

		// Import the file after ensuring it does not already exist on the database
		if (Utility.FilenameIsInLog(downloadFileName) == false)
		{
			ZipArchive archive = Utility.UnzipUserInputFileFromWeb(sourcePath + downloadFileName, client);
			if (archive != null)
			{
				LoadObjectsIntoDatabase(archive, downloadFileName);

				if (archivingOn)
				{
					SaveArchive(client, sourcePath + downloadFileName, archivePath + "\\" + downloadFileName);
				}
			}
		}
		else
		{
			if (log.IsWarnEnabled) log.Warn(downloadFileName + " has already been logged. Please clear this log entry if you want to import this file.");
		}
	}
}

/// <summary>
/// Inserts data from an unzipped file into Oracle table
/// </summary>
public void LoadObjectsIntoDatabase(ZipArchive archive, string filename)
{
	var objPosts = ReadArchiveIntoObjects(archive, filename);
	DataTable dataTable = ToDataTable(objPosts);
	if (InsertDataBulkCopy(dataTable))
	{
		Utility.WriteFilenameToDb(filename);
	}

}

// Parses a file into Post objects
public List<Post> ReadArchiveIntoObjects(ZipArchive archive, string filename)
{
	var objPosts = new List<Post>();
	if (archive != null)
	{
		foreach (ZipArchiveEntry entry in archive.Entries)
		{
			if (entry.FullName.Contains("xml"))
			{
				XDocument xDocument = null;
				XmlReaderSettings xmlReaderSettings = new XmlReaderSettings { CheckCharacters = false };
				using (XmlReader xmlReader = XmlReader.Create(entry.Open(), xmlReaderSettings))
				{
					// Load our XDocument
					xmlReader.MoveToContent();
					xDocument = XDocument.Load(xmlReader);
				}

				foreach (XElement xElement in xDocument.Elements(xDocument.Root.Name).DescendantsAndSelf())
				{
					IEnumerable<XElement> posts =
						from p in xElement.Elements("posts").Elements("post")
						select (XElement)p;

					foreach (XElement post in posts)
					{

						//replace UTC with Z in order to parse into datetime format
						string publishedon = (string)post.Element("published_on");
						DateTime dt = DateTime.Parse(publishedon.Trim().Replace("UTC", "Z"));

						var _post = new Post
							{
								TITLE = (string)post.Element("title"),
								LINK = (string)post.Element("link"),
								MESSAGE_ID = (string)post.Element("message_id"),
								PUBLISHED_ON = (OracleDate)dt,
								SOURCE_TYPE = (string)post.Element("source_type"),
								AUTHOR_NAME = (string)post.Element("author").Element("name"),
								AUTHOR_COUNTRY = (string)post.Element("author").Element("country"),
								AUTHOR_STATE = (string)post.Element("author").Element("state"),
								AUTHOR_CITY = (string)post.Element("author").Element("city"),
								AUTHOR_BIRTH_YEAR = (int?)post.Element("author").Element("birth_year"),
								AUTHOR_GENDER = (string)post.Element("author").Element("gender"),
								KLOUT_SCORE = (int?)post.Element("author").Element("klout_score"),
								FOLLOWERS_COUNT = (int?)post.Element("author").Element("followers_count"),
								FILE_NUMBER = Utility.GetRegexMatch(filename, @"\d{8}_\d{3}")
							};

						IEnumerable<XElement> topics = from p in post.Elements("topics").Elements("topic") select (XElement)p;

						// For multiple topics, it has been decided that duplicate posts should be created.
						foreach (XElement topic in topics)
						{
							var clone = _post.Clone();
							clone.TOPIC_NAME = (string)topic.Attribute("name");
							clone.TOPIC_ID = (int?)topic.Attribute("id");

							clone.TOPIC_RANK = 0; ;
							clone.TOPIC_TONALITY = 0.0;


							IEnumerable<XElement> snippets = from p in topic.Elements("snippets").Elements("snippet") select (XElement)p;
							foreach (XElement snippet in snippets)
							{
								clone.SNIPPET_ID = (long?)snippet.Element("id");
								clone.SNIPPET_TEXT = (string)snippet.Element("text");
								clone.SNIPPET_READABILITY = (string)snippet.Element("readability");
								clone.SNIPPET_TONALITY = (int?)snippet.Element("tonality");
								clone.SNIPPET_ANCHOR = (string)snippet.Element("anchor");
								IEnumerable<XElement> dimensions = from p in snippet.Elements("dimensions").Elements("dimension") select (XElement)p;

								foreach (XElement dimension in dimensions)
								{
									clone.DIMENSION_ID = (int?)dimension.Element("id");
									clone.DIMENSION_NAME = (string)dimension.Element("name");
								}
							}

							clone.CREATE_TS = (OracleDate)DateTime.Now;

							objPosts.Add(clone);
						}

					}
				}
			}
		}
	}
	return objPosts;
}




public static class Utility
{
	private static log4net.ILog log = log4net.LogManager.GetLogger(typeof(Utility));

	public enum DataSourceType { FromFile, FromUrl };
	// NOTE: Modified these strings for confidentiality
	private const string CollectiveIntellectBaseUrl = "<url here>";

	private const string CollectiveIntellectSite =
		"<url here>";

	private const string WebClientUserAgent =
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.0.3705;)";

	public static ZipArchive UnzipUserInputFileFromWeb(string fileSource, WebClient client)
	{
		if (log.IsInfoEnabled) log.Info("Trying to open the file...");
		try
		{
			// Unzip a file using Microsoft ZipArchive
			Stream zipData = client.OpenRead(fileSource);
			ZipArchive archive = new ZipArchive(zipData, ZipArchiveMode.Read);
			if (log.IsInfoEnabled) log.Info("File opened successfully.");

			return archive;
		}
		catch (WebException ex)
		{
			string fileFormat = GetCIFileFormat();
			if (log.IsErrorEnabled) log.Error(
				String.Format(
					"Something went wrong while trying to download the file from Collective Intellect. Please try again after making sure that the file name is listed on {0} and matches this format: {1}",
					CollectiveIntellectSite, fileFormat), ex);
			return null;
		}
		catch (Exception ex)
		{
			if (log.IsErrorEnabled) log.Error("Something went wrong while trying to open the file from Collective Intellect. Please try again.", ex);
			return null;
		}
	}

	public static bool? FilenameIsInLog(string filename)
	{
		try
		{
			using (var con = new OracleConnection(ConnectionProvider.GetConnectionString()))
			{
				con.Open();
				using (
					OracleCommand cmd =
						new OracleCommand(
							"<stored procedure here>", con)
					)
				{
					cmd.CommandType = CommandType.StoredProcedure;
					using (OracleParameter in_filename = new OracleParameter())
					{
						in_filename.OracleDbType = OracleDbType.Varchar2;
						in_filename.Direction = ParameterDirection.Input;
						in_filename.Value = filename;
						cmd.Parameters.Add(in_filename);
						OracleParameter filename_exists_c = cmd.Parameters.Add("filename_exists_c", OracleDbType.RefCursor);
						filename_exists_c.Direction = ParameterDirection.Output;
						OracleDataReader dr = cmd.ExecuteReader();
						if (dr.Read())
						{
							if (log.IsInfoEnabled) log.Info(string.Format("{0} has already been imported, according to the log. Skipping import of this file.", filename));
							return true;
						}
						else return false;
					}
				}
			}
		}
		catch (Exception ex)
		{
			if (log.IsErrorEnabled) log.Error(string.Format("Something went wrong while trying to check if {0} has been logged. I'll assume the file is already logged and skip it this time. Please try again later.", filename), ex);
			return true;
		}
	}


	public static void WriteFilenameToDb(string filename)
	{
		try
		{
			using (var con = new OracleConnection(ConnectionProvider.GetConnectionString()))
			{
				con.Open();
				using (
					OracleCommand cmd =
						new OracleCommand(
							"<stored procedure here>", con)
					)
				{
					cmd.CommandType = CommandType.StoredProcedure;
					using (OracleParameter filename_insert = new OracleParameter())
					{
						filename_insert.OracleDbType = OracleDbType.Varchar2;
						filename_insert.Direction = ParameterDirection.Input;
						filename_insert.Value = filename;
						cmd.Parameters.Add(filename_insert);
						cmd.ExecuteNonQuery();
					}
				}
			}
			if (log.IsInfoEnabled) log.Info(filename + " was imported successfully.");
		}
		catch (Exception ex)
		{
			if (log.IsErrorEnabled) log.Error(string.Format("Something went wrong while trying to add the filename {0} to the log.", filename), ex);
		}
	}
}
