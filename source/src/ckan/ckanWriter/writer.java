package ckan.ckanWriter;

import COM.safe.fme.pluginbuilder.*;
import COM.safe.fmeobjects.*;

public final class writer implements IFMEWriterCreator
{
	/**
	 * Creates a new writer object (that implements the IFMEWriter interface).
	 * Note that any Exception thrown in this method will cause a message to be
	 * displayed on standardout, in addition to the Exception's specific message
	 * text. As well, both messages will be displayed in the log file.
	 * 
	 * @param mappingFile the mapping file object to be used by this IFMEWriter.
	 * @param logFile the log file object to be used by this IFMEWriter.
	 * @return The instance of the writer object.
	 * @throws Exception if a problem is encountered during creation of the reader.
	 */
	@Override
	public IFMEWriter createWriter(IFMEMappingFile mappingFile,
									IFMELogFile logFile,
									IFMECoordSysManager coordSysMan,
									IFMESession session, String writerTypeName,
									String writerKeyword) throws Exception
	{
		ckanWriter writerInstance = new ckanWriter(writerTypeName, writerKeyword);
		writerInstance.init(mappingFile, logFile, coordSysMan, session);
		return writerInstance;
	}
}