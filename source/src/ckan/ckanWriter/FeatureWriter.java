package ckan.ckanWriter;

import COM.safe.fme.pluginbuilder.*;
import COM.safe.fmeobjects.*;

import java.io.*;
import java.util.*;
import java.text.Normalizer;  
import java.text.Normalizer.Form;  
import java.util.Locale;  
import java.util.regex.Pattern;

import ckan.CKANclient.CKANException;
import ckan.CKANclient.Client;
import ckan.CKANclient.Connection;
import ckan.CKANclient.Dataset;
import ckan.CKANclient.Resource;
import ckan.CKANclient.DataStore;
import ckan.CKANclient.Field;

public class FeatureWriter
{  
	// ------------------------------------------------------------------
	// Declare private data members (by convention these are identified
	// by a trailing underscore).
	//

	// The gLogFile member stores a copy of an IFMELogFile object that
	// allows the plug-in to log messages to the FME log file.
	// It is initialized externally after the plug-in object is created.
	private IFMELogFile gLogFile = null;

	// The gMappingFile member stores a copy of an IFMEMappingFile object
	// that allows the plug-in to access information from the mapping file.
	// It is initialized externally after the plug-in object is created.
	private IFMEMappingFile gMappingFile = null;

	// The gCoordSysman member stores a copy of an IFMECoordSysManager
	// object that allows the plug-in to retrieve and define
	// coordinate systems. it is initialized after the plug-in object
	// is created.
	private IFMECoordSysManager gCoordSysMan = null;

	// The fmeSession_ member stores a copy of an IFMESession object
	// which performs the services on the FME objects.
	private IFMESession fmeSession_ = null;

	// The writerTypeName_ is the value specified for WRITER_TYPE in
	// the mapping file. It is also specified for the FORMAT_NAME in
	// the metafile. This member is initialized in the class constructor
	// to the value passed in by the FME.
	private String writerTypeName_ = "";

	// The writerKeyword_ is typically the same as the writerTypeName_, but
	// it could be different if the value was specified for WRITER_KEYWORD in
	// the FME mapping file. For example, someone might explicitly request:
	// WRITER_TYPE SHAPE
	// WRITER_KEYWORD SHAPE_OUT
	// in the mapping file, in which case writerKeyword_ is
	// "SHAPE_OUT" while the writerTypeName_ is SHAPE. this member is
	// initialized in the class constructor to the value passed in by FME
	private String writerKeyword_ = "";

	// The dataset_ member stores the name of the dataset that the plug-in is
	// writing to. it is initialized to an empty string in the constructor and
	// set to the value passed in by FME in the open() method.
	private String dataset_ = "";

	// The outputFile_ member stores the output file stream for the output dataset.
	//private static BufferedWriter outputFile_ = null;
	private PrintStream outputFile_ = null;

	// Array of possible attribute names.
	private ArrayList<String> attributeNames_ = null;
	private Map<String, String> attributeTypes_ = null;
	
	private String domain_ = "";
	private String apiKey_ = "";
	private String packageId_ = "";
	private String resourceName_ = "";
	private String resourceDescription_ = "";
	private String updateResource_ = "NO";
	private String datastore_ = "NO";
	private String resourceId_ = "";
	private String result_id = "";
	
	private List<Field> fields = new ArrayList<Field>();
	private LinkedHashMap<String, Object> ckanRow = new LinkedHashMap<String, Object>();
	private List<LinkedHashMap<String, Object>> records = new ArrayList<LinkedHashMap<String, Object>>();
	
	private static final Pattern NONLATIN = Pattern.compile("[^\\w-]");
	private static final Pattern WHITESPACE = Pattern.compile("[\\s]");
	
	/**
	 * Constructor
	 * @param writerTypeName  The name of this particular writer
	 * @param writerKeyword   The keyword of this particular writer
	 */
	public FeatureWriter(final String writerTypeName, final String writerKeyword)
	{
		writerTypeName_ = writerTypeName;
		writerKeyword_ = writerKeyword;
	}

	/**
	 * Initializes the writer with the mapping file, log file, coordinate system
	 * member and session objects.
	 * @param mappingFile The mapping file object
	 * @param logFile     The log file object
	 * @param coordSysMan The coordinate system manger object
	 * @param session     The session object
	 * @throws Exception  In the event that anything goes awry
	 */
	public void init(IFMEMappingFile mappingFile, IFMELogFile logFile,
			IFMECoordSysManager coordSysMan, IFMESession session) throws Exception
	{
		gMappingFile = mappingFile;
		gLogFile = logFile;
		gCoordSysMan = coordSysMan;
		fmeSession_ = session;

		// To remove all warnings from compilation, we log the writer name
		// and its keyword.
		gLogFile.logMessageString("Writer Name: " + writerTypeName_, IFMELogFile.FME_INFORM);
		gLogFile.logMessageString("Writer Keyword: " + writerKeyword_, IFMELogFile.FME_INFORM);
	}

	/**
	 * Execute before opening file for writing
	 * @param datasetName The name of the dataset
	 */
	private void openSetup(String datasetName)
	{
		// Get the name of the file we're writing to
		dataset_ = datasetName;  
	}

	/**
	 * This method opens the data streams and sets up the data members for
	 * "Single Channel" writing.
	 * @param parameters  The parameters for the writer
	 */
	public void openAdvance(ArrayList<String> parameters)
	{
		// Perform setup steps before opening file for writing
		openSetup(parameters.get(0));

		// Append kFeatureExtension if it isn't there
		if((dataset_.length() < 4) || (!dataset_.substring(dataset_.length() - 4, dataset_.length()).equals(".csv")))
		{
			dataset_ += ".csv";
		}

		// Create a useful opening writer file message
		String msgOpeningWriter = "Opening the CKAN Writer on dataset" + dataset_;
		gLogFile.logMessageString(msgOpeningWriter, IFMELogFile.FME_INFORM);

		try
		{
			File myfile = new File(dataset_);

			myfile.delete();
			myfile.createNewFile();

			outputFile_ = new PrintStream(new FileOutputStream(myfile));
			
			// Read the values entered from the settings box.
			readSettingsBoxKeywordValues();
			
			// Create a CKAN package, if this fails it means the package already exists
			createCkanPackage();
			
		    // Write the schema information to the file.
		    writeSchemaFeature(parameters);
		}
		catch (IOException e)
		{
			// Create a useful error message stating the directory/file
			String msgErrorOpeningWriter = "Error opening the CKAN Writer on dataset" + dataset_;
			gLogFile.logMessageString(msgErrorOpeningWriter, IFMELogFile.FME_ERROR);
			return;
		}
	}

	/**
	 * This method closes the file stream. This method is designed so
	 * that it can be called many times for a single openBasic() or
	 * openAdvance() call and have the same effect of being called
	 * only once.
	 */
	public void closeWriter()
	{
		// Upload the file to CKAN
        if (updateResource_.equals("NO"))
		{
			createCkanResource();
		}
		else
		{
			updateCkanResource();
		}
        
		// If we have already closed the file, quit
		if(outputFile_ == null)
			return;

		outputFile_.close();

		// Log that the writer is done
		gLogFile.logMessageString("Closing the CKAN Writer", IFMELogFile.FME_INFORM);
	}

	/**
	 * This method takes "feature" and writes it's geometry and
	 * attributes to the output dataset.
	 * @param feature The feature to write
	 */
	public void writeFeature(IFMEFeature feature)
	{
		ArrayList<String> featAttrNames = new ArrayList<String>();
		String attrName = "";
		String attrValue = "";
		String outValue = "";
		String outType = "";

		feature.getAllAttributeNames(featAttrNames);
		
		Map<String, String> attributeValues_ = new HashMap<String, String>();
		boolean firstColumn = true;
		
		// Go through all the attributes and add their names and values only
		// if the attributes are defined in the attribute definition header.
		for (int i = 0; i < featAttrNames.size(); i++)
		{
			attrName = (String)featAttrNames.get(i);
			try
			{
				attrValue = feature.getStringAttribute(attrName);
				
				// Only add the attribute if it's a user attribute
				if(attributeNames_.contains(attrName))
				{
					attributeValues_.put(attrName, attrValue);
				}
			}
			catch (FMEException e)
			{
				gLogFile.logMessageString("An attribute defined in a feature does not have an attribute value associated with it.", IFMELogFile.FME_WARN);
			}
		}

		// Go through all the attributes in order and write out add their values
		for (int i = 0; i < attributeNames_.size(); i++)
		{
			outType = attributeTypes_.get(attributeNames_.get(i));
			outValue = "";
			
			// Add comma delimiter if not the first column
			if(firstColumn) {
				firstColumn = false;
			}
			else {
				outValue = ",";
			}
			
			if(attributeValues_.get(attributeNames_.get(i)) == null)
			{
				// Empty String
			}
			else if(outType.contains("char") || outType.contains("string") || outType.contains("text"))
			{
				outValue = outValue + "\"" + attributeValues_.get(attributeNames_.get(i)) + "\"";
			}
			else
			{
				outValue = outValue + attributeValues_.get(attributeNames_.get(i));
			}
			outputFile_.print(outValue);
		}
		outputFile_.println();
	}

	/**
	 * This method writes the schema feature information to the output
	 * dataset file. "parameters" is an array containing the feature type
	 * definition as explained in the openAdvace() description.
	 * @param parameters  An array of parameters for schema definition
	 */
	private void writeSchemaFeature(ArrayList<String> parameters)
	{
		// Run through the parameters array and use those values to generate
		// the schema feature which we write to the header of the dataset file.

		// We're going to have attribute names, so lets set up our ArrayList here
		attributeNames_ = new ArrayList<String>();
		attributeTypes_ = new HashMap<String, String>();
		
		String paramVal;
		String paramType;
		
		if (datastore_.equals("NO"))
		{
			boolean firstColumn = true;
			
			// Write the attribute definition to file now.
			for(int i=4;i<parameters.size();i+=2)
			{
				// Grab the attribute name and write it to the file.
				paramVal = parameters.get(i);
				paramType = parameters.get(i+1);
				
				attributeNames_.add(paramVal);
				attributeTypes_.put(paramVal, paramType);
				
				if(firstColumn) {
					paramVal = "\"" + paramVal + "\"";
					firstColumn = false;
				}
				else {
					paramVal = ",\"" + paramVal + "\"";
				}
				outputFile_.print(paramVal);
			}
			outputFile_.println();
		}
		else
		{
			// Write the attribute definition to Datastore.
			for(int i=4;i<parameters.size();i+=2)
			{
				// Grab the attribute name.
				paramVal = parameters.get(i);
				paramType = parameters.get(i+1);
				
				attributeNames_.add(paramVal);
				attributeTypes_.put(paramVal, paramType);
				
				fields.add(new Field(paramVal));
			}
		}
	}

	/**
	 * A settings box acts as the bridge between a mapping file and the
	 * reader/writer. It should contain any configurable parameters defined
	 * for a reader/writer. In the reader, the parameter values
	 * are accessed through keywords that identify the parameters.
	 */
	private void readSettingsBoxKeywordValues()
	{
		domain_ = "";
		apiKey_ = "";
		packageId_ = "";
		resourceName_ = "";
		resourceDescription_ = "";
		updateResource_ = "NO";
		datastore_ = "NO";
		resourceId_ = "";

		// Determine if there is a mapping to "_DESTINATION_DOMAIN" which is specified
		// in the metafile. If the User ID is not entered in the settings box, a 
		// mapping is not generated in the mapping file.
		try
		{
			// Mapping has been found, set the user id string and log the data
			domain_ = gMappingFile.fetchString("_DESTINATION_DOMAIN");
			if(domain_.length() != 0)
			{
				if(!domain_.startsWith("http://"))
				{
					domain_ = "http://" + domain_;
				}
			    while (domain_.endsWith("/") && domain_ != "http://") {
			    	domain_ = domain_.substring(0,domain_.length()-1);
			    }
				gLogFile.logMessageString(domain_, IFMELogFile.FME_INFORM);
			}
		}
		catch (Exception e)
		{
			// No mapping found.
			gLogFile.logMessageString("No valid domain was entered.", IFMELogFile.FME_INFORM);
		}

		// Determine if there is a mapping to "APIKEY" which is specified
		// in the metafile.
		try
		{
			// Mapping was found, set the api key string and log the data.
			apiKey_ = gMappingFile.fetchString("_DESTINATION_APIKEY");
			gLogFile.logMessageString("API Key: "+apiKey_, IFMELogFile.FME_INFORM);
		}
		catch (Exception e)
		{
			// No mapping found.
			gLogFile.logMessageString("No valid API key was entered.", IFMELogFile.FME_INFORM);
		}
		
		// Determine if there is a mapping to "PACKAGEID" which is specified
		// in the metafile.
		try
		{
			// Mapping was found, set the package id string and log the data.
			packageId_ = gMappingFile.fetchString("_PACKAGE_ID");
			if(packageId_.length() != 0)
			{
				packageId_ = toSlug(packageId_);
				gLogFile.logMessageString("Package ID: "+packageId_, IFMELogFile.FME_INFORM);
			}
		}
		catch (Exception e)
		{
			// No mapping found.
			gLogFile.logMessageString("No valid package ID was entered.", IFMELogFile.FME_INFORM);
		}
		
		// Determine if there is a mapping to "RESOURCENAME" which is specified
		// in the metafile.
		try
		{
			// Mapping was found, set the package id string and log the data.
			resourceName_ = gMappingFile.fetchString("_RESOURCE_NAME");
			if(resourceName_.length() != 0)
			{
				resourceName_ = resourceName_.trim();
				gLogFile.logMessageString("Resouce Name: "+resourceName_, IFMELogFile.FME_INFORM);
			}
		}
		catch (Exception e)
		{
			// No mapping found.
			gLogFile.logMessageString("No valid resource title was entered.", IFMELogFile.FME_INFORM);
		}
		
		// Determine if there is a mapping to "RESOURCEDESCRIPTION" which is specified
		// in the metafile.
		try
		{
			// Mapping was found, set the resource description string and log the data.
			resourceDescription_ = gMappingFile.fetchString("_RESOURCE_DESCRIPTION");
			if(resourceDescription_.length() != 0)
			{
				resourceDescription_ = resourceDescription_.trim();
				gLogFile.logMessageString(resourceDescription_, IFMELogFile.FME_INFORM);
			}
		}
		catch (Exception e)
		{
			// No mapping found.
			gLogFile.logMessageString("No resource description was entered.", IFMELogFile.FME_INFORM);
		}

		// Determine if there is a mapping to "UPDATE_RESOURCE" which is specified
		// in the metafile.
		try
		{
			// Mapping was found, set the UPDATE RESOURCE field and log the data.
			updateResource_ = gMappingFile.fetchString("_UPDATE_RESOURCE");
			gLogFile.logMessageString("Update resource? "+updateResource_, IFMELogFile.FME_INFORM);
		}
		catch (Exception e)
		{
			// No mapping found.
			gLogFile.logMessageString("Unable to determine if resource is to be updated.", IFMELogFile.FME_INFORM);
		}

		// Determine if there is a mapping to "RESOURCE_ID" which is specified
		// in the metafile.
		try
		{
			// Mapping was found, set the resource ID string and log the data.
			resourceId_ = gMappingFile.fetchString("_RESOURCE_ID");
			if(resourceId_.length() != 0)
			{
				resourceId_ = resourceId_.trim().toLowerCase().replaceAll("\\s","-");
				gLogFile.logMessageString("Resource ID: "+resourceId_, IFMELogFile.FME_INFORM);
			}
		}
		catch (Exception e)
		{
			// No mapping found.
			gLogFile.logMessageString("No resource ID was entered", IFMELogFile.FME_INFORM);
		}
		
		// Determine if there is a mapping to "DATASTORE" which is specified
		// in the metafile.
		try
		{
			// Mapping was found, set the DATASTORE field and log the data.
			datastore_ = gMappingFile.fetchString("_DATASTORE");
			gLogFile.logMessageString("Use datastore? "+datastore_, IFMELogFile.FME_INFORM);
		}
		catch (Exception e)
		{
			// No mapping found.
			gLogFile.logMessageString("Unable to determine if resource is to be updated.", IFMELogFile.FME_INFORM);
		}
	}
	
    public String toSlug(String input) {  
        String nowhitespace = WHITESPACE.matcher(input).replaceAll("-");
        String normalized = Normalizer.normalize(nowhitespace, Form.NFD);  
        String slug = NONLATIN.matcher(normalized).replaceAll("");
        return slug.toLowerCase(Locale.ENGLISH).replaceAll("-{2,}","-").replaceAll("^-|-$","");
    }
	
	private void createCkanPackage(){
		Client ckanClient = new Client( new Connection(domain_), apiKey_);
		
        try {
        	Dataset ds = new Dataset();
        	
            ds.setId(packageId_);
            ds.setName(packageId_);

            Dataset result = ckanClient.createDataset(ds);
            gLogFile.logMessageString("Package created.", IFMELogFile.FME_INFORM);
        } catch ( CKANException cke ) {
        	gLogFile.logMessageString(cke.toString(), IFMELogFile.FME_INFORM);
            return;
        } catch ( Exception e ) {
        	gLogFile.logMessageString(e.toString(), IFMELogFile.FME_ERROR);
            return;
        }
	}
	
	private void createCkanResource(){
		Client ckanClient = new Client( new Connection(domain_), apiKey_);
		
        try {
        	Resource rs = new Resource();
        	
            rs.setPackage_id(packageId_);
            rs.setName(resourceName_);
            rs.setDescription(resourceDescription_);
            rs.setFormat("CSV");
            rs.setMimetype("text/csv");
            rs.setUrl("");

            Resource result = ckanClient.uploadCreateResource(rs,dataset_);
            result_id = result.getId();
            gLogFile.logMessageString("Resource created at "+ domain_ +"/dataset/"+ packageId_ +"/resource/"+result_id, IFMELogFile.FME_INFORM);
        } catch ( CKANException cke ) {
        	gLogFile.logMessageString(cke.toString(), IFMELogFile.FME_ERROR);
        	gLogFile.logMessageString("Please check your Writer parameters.", IFMELogFile.FME_ERROR);
            return;
        } catch ( Exception e ) {
        	gLogFile.logMessageString(e.toString(), IFMELogFile.FME_ERROR);
            return;
        }
	}

	private void updateCkanResource(){
		Client ckanClient = new Client( new Connection(domain_), apiKey_);
		
        try {
        	Resource rs = new Resource();
        	
            rs.setPackage_id(packageId_);
            rs.setName(resourceName_);
            rs.setDescription(resourceDescription_);
            rs.setFormat("CSV");
            rs.setMimetype("text/csv");
            rs.setUrl("");
            rs.setId(resourceId_);

            Resource result = ckanClient.uploadUpdateResource(rs,dataset_);
            result_id = result.getId();
            gLogFile.logMessageString("Resource updated at "+ domain_ +"/dataset/"+ packageId_ +"/resource/"+result_id, IFMELogFile.FME_INFORM);
        } catch ( CKANException cke ) {
        	gLogFile.logMessageString(cke.toString(), IFMELogFile.FME_ERROR);
        	gLogFile.logMessageString("Please check your Writer parameters.", IFMELogFile.FME_ERROR);
            return;
        } catch ( Exception e ) {
        	gLogFile.logMessageString(e.toString(), IFMELogFile.FME_ERROR);
            return;
        }
	}
	
	public void newDataStore(List<Field> fields, List<LinkedHashMap<String, Object>> records) {
		Client ckanClient = new Client( new Connection(domain_), apiKey_);
		
        try {
        	DataStore ds = new DataStore();
            
        	ds.setFields(fields);
            ds.setRecords(records);
            ds.setResource_id(resourceId_);
            ds.setForce("True");
            
            DataStore result = ckanClient.createDataStore(ds,5);
            return;
        } catch ( CKANException cke ) {
        	gLogFile.logMessageString(cke.toString(), IFMELogFile.FME_ERROR);
        	return;
        }
	}
	
	public void uploadDataStore(List<LinkedHashMap<String, Object>> records) {
		Client ckanClient = new Client( new Connection(domain_), apiKey_);
		
        try {
        	DataStore ds = new DataStore();
        	
            ds.setRecords(records);
            ds.setResource_id(resourceId_);
            ds.setForce("True");
            ds.setMethod("insert");
            
            DataStore result = ckanClient.upsertDataStore(ds,5);
            return;
        } catch ( CKANException cke ) {
        	gLogFile.logMessageString(cke.toString(), IFMELogFile.FME_ERROR);
        	return;
        }
	}
}
