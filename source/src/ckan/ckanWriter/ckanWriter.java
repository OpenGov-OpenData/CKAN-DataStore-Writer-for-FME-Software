package ckan.ckanWriter;

import COM.safe.fme.pluginbuilder.*;
import COM.safe.fmeobjects.*;

import java.util.*;

public final class ckanWriter implements IFMEWriter
{
	// ------------------------------------------------------------------
	// Declare private data members (by convention these are identified
	// by a trailing underscore).
	//

	// The writer_ member is an object that does the actual writing 
	// of the data file.
	private FeatureWriter writer_ = null;

	// This constant holds the writer ID assigned by Safe Software for
	// your writer -- you may use this ID during your development, but
	// before you go into production, you should contact Safe Software
	// for you permanent ID.
	private int kFeatureWriterId = 87062;

	/**
	 * The writerTypeName is the named used in the following contexts:
	 * - the name of the format's .db file in the formatsinfo folder
	 * - the format short name for the format within .db file
	 * - FORMAT_NAME in the metafile
	 * 
	 * The writerKeyword is a unique identifier for this writer instance.
	 * It is usually set by the WRITER_KEYWORD in the mapping file.
	 * 
	 * Since there can be multiple instances of a writerTypeName within
	 * an FME session, the writerKeyword must be used when fetching
	 * context information from FME.
	 * @param writerTypeName  The type name of this writer
	 * @param writerKeyword The keyword of this writer
	 */
	public ckanWriter(String writerTypeName, String writerKeyword)
	{
		writer_ = new FeatureWriter(writerTypeName,writerKeyword);
	}

	/**
	 * Initializes the writer with the mapping file, log file, coordinate
	 * system manager and session objects
	 * @param mappingFile The mapping file
	 * @param logFile     The log file
	 * @param coordSysMan The coordinate system manager
	 * @param session     The session
	 * @throws Exception  In the event anything goes wrong
	 */
	public void init(IFMEMappingFile mappingFile, IFMELogFile logFile,
					IFMECoordSysManager coordSysMan, IFMESession session) throws Exception
	{
		writer_.init(mappingFile, logFile, coordSysMan, session);
	}

	/**
	 * open()
	 */
	@Override
	public void open(ArrayList<String> parameters) throws Exception
	{
		writer_.openAdvance(parameters);
	}

	/**
	 * abort()
	 */
	@Override
	public void abort() throws Exception
	{
		close();
	}

	/**
	 * close()
	 */
	@Override
	public void close() throws Exception
	{
		writer_.closeWriter();
	}

	/**
	 * write()
	 */
	@Override
	public void write(IFMEFeature feature) throws Exception
	{
		writer_.writeFeature(feature);
	}

	/**
	 * multiFileWriter()
	 */
	@Override
	public boolean multiFileWriter()
	{
		return true;
	}

	/**
	 * id()
	 */
	@Override
	public int id()
	{
		return kFeatureWriterId;
	}

	/**
	 * commitTransaction()
	 */
	@Override
	public void commitTransaction() throws Exception
	{
	}

	/**
	 * startTransaction()
	 */
	@Override
	public void startTransaction() throws Exception
	{
	}

	/**
	 * rollbackTransaction()
	 */
	@Override
	public void rollbackTransaction() throws Exception
	{
	}

    /**
    * addMappingFileDefLine()
    */
    @Override
    public void addMappingFileDefLine(ArrayList<String> defLine) throws Exception
    {
    }
}
