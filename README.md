# CKAN DataStore Writer for FME Software
CKAN DataStore Writer for FME® Software


### Installation

Copy the files below to their respective folders under the FME installation directory:
- ../FME/formatsinfo/ckan\_datastore.db
- ../FME/metafile/ckan.datastore.writer.fmf
- ../FME/plugins/ckan\_datastore.jar
- ../FME/plugins/gson-2.2.jar
- ../FME/plugins/httpclient-4.2.jar
- ../FME/plugins/httpcore-4.2.jar
- ../FME/plugins/httpmime-4.2.jar


### User Guide

The CKAN DataStore writer plugin will upload the output of a FME workflow to a CKAN data portal.

To add a new writer to the FME Workbench click on the Writers menu located at the top and click Add Writer.
(As an alternative the shortcut Ctrl+Alt+W can be used)

In the window labled "Add Writer" go to the Format field and select "CKAN DataStore Writer" from the dropdown menu.
Next click on the Parameters button to enter the following:

**Domain:** Url of the CKAN data portal (eg: http://demo.ckan.org)

**API Key:** The API key can be found the in the user profile on the CKAN data portal

**Package ID:** The ID of an existing package where the output file will be uploaded as a new resource (eg: test-dataset)

**Resoure Title:** The name of the new resource

**Description:** A short description about the resource (optional)

**Update Existing Resource:** Click on this checkbox and enter the resource id to update an existing resource

**Advanced Settings:** You can change the _Batch Size_ and _Primary Key_ here

**Batch Size:** The writer will upload rows of data in batches of this amount

**Primary Key:** If a Primary Key is specified then data will be upserted instead of inserted  
	Multiple fields can be specified as the Primary Key, use double semicolons to delineate the fields (eg: field1;;field2)
