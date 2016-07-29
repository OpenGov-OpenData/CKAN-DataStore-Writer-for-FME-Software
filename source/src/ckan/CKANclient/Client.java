package ckan.CKANclient;

import com.google.gson.Gson;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

/**
 * The primary interface to this package the Client class is responsible for
 * managing all interactions with a given connection.
 *
 * @author Ross Jones <ross.jones@okfn.org>
 * @version 1.7
 * @since 2012-05-01
 */
public final class Client {

	private Connection _connection = null;

	/**
	 * Constructs a new Client for making requests to a remote CKAN instance.
	 *
	 * @param c			A Connection object containing info on the location of the CKAN Instance.
	 * @param apikey	A user's API Key sent with every request.
	 */
	public Client(Connection c, String apikey) {
		this._connection = c;
		this._connection.setApiKey(apikey);
	}

	/**
	 * Loads a JSON string into a class of the specified type.
	 */
	protected <T> T LoadClass(Class<T> cls, String data) {
		Gson gson = new Gson();
		return gson.fromJson(data, cls);
	}

	/**
	 * Handles error responses from CKAN
	 *
	 * When given a JSON string it will generate a valid CKANException
	 * containing all of the error messages from the JSON.
	 *
	 * @param json		The JSON response
	 * @param action	The name of the action calling this for the primary error message.
	 * @throws A CKANException containing the error messages contained in the provided JSON.
	 */
	private void HandleError(String json, String action) throws CKANException {

		CKANException exception = new CKANException("Errors occured performing: " + action);

		HashMap hm = LoadClass(HashMap.class, json);
		Map<String, Object> m = (Map<String, Object>) hm.get("error");
		for (Map.Entry<String, Object> entry : m.entrySet()) {
			if (entry.getKey().startsWith("_"))
				continue;
			exception.addError(entry.getKey() + " - " + entry.getValue());
		}
		throw exception;
	}

	/**
	 * Handles html error responses from CKAN
	 */
	private void HandleHtmlError(String text, String action) throws CKANException {
		CKANException exception = new CKANException("Errors occured performing: " + action);
		
		Document html = Jsoup.parse(text);
		String h1 = html.body().getElementsByTag("h1").text();
		exception.addError(h1);
		
		throw exception;
	}
	
	/**
	 * Retrieves a dataset
	 *
	 * Retrieves the dataset with the given name, or ID, from the CKAN
	 * connection specified in the Client constructor.
	 *
	 * @param name		The name or ID of the dataset to fetch
	 * @returns The Dataset for the provided name.
	 * @throws A CKANException if the request fails
	 */
	public Dataset getDataset(String name) throws CKANException {
		String returned_json = this._connection.Post("/api/action/package_show", "{\"id\":\"" + name + "\"}");
		Dataset.Response r = LoadClass(Dataset.Response.class, returned_json);
		if (!r.success) {
			HandleError(returned_json, "getDataset");
		}
		return r.result;
	}

	/**
	 * Deletes a dataset
	 *
	 * Deletes the dataset specified with the provided name/id
	 *
	 * @param name		The name or ID of the dataset to delete
	 * @throws A CKANException if the request fails
	 */
	public void deleteDataset(String name) throws CKANException {
		String returned_json = this._connection.Post("/api/action/package_delete", "{\"id\":\"" + name + "\"}");
		Dataset.Response r = LoadClass(Dataset.Response.class, returned_json);
		if (!r.success) {
			HandleError(returned_json, "deleteDataset");
		}
	}

	/**
	 * Creates a dataset on the server
	 *
	 * Takes the provided dataset and sends it to the server to perform an
	 * create, and then returns the newly created dataset.
	 *
	 * @param dataset	A dataset instance
	 * @returns The Dataset as it now exists
	 * @throws A CKANException if the request fails
	 */
	public Dataset createDataset(Dataset dataset) throws CKANException {
		Gson gson = new Gson();
		String data = gson.toJson(dataset);
		System.out.println(data);
		String returned_json = this._connection.Post("/api/action/package_create", data);
		System.out.println(returned_json);
		Dataset.Response r = LoadClass(Dataset.Response.class, returned_json);
		if (!r.success) {
			// This will always throw an exception
			HandleError(returned_json, "createDataset");
		}
		return r.result;
	}

	/**
	 * Creates a resource on the server through a file upload
	 *
	 * Takes the provided resource and sends it to the server to perform an
	 * create.
	 *
	 * @param resource	A resource instance
	 * @throws IOException
	 * @returns The Resource as it now exists
	 * @throws A CKANException if the request fails
	 */
	public Resource uploadCreateResource(Resource resource, String filePath) throws CKANException {
		String returned_json = this._connection.ResourceMultiPartPost("/api/action/resource_create", resource, filePath);
		System.out.println(returned_json);
		if (returned_json.lastIndexOf("<html>", 0) != 0) {
			Resource.Response r = LoadClass(Resource.Response.class, returned_json);
			if (!r.success) {
				// This will always throw an exception
				HandleError(returned_json, "createResource");
			}
			return r.result;
		}
		else {
			HandleHtmlError(returned_json, "createResource");
			return resource;
		}
	}
	
	/**
	 * Creates a resource on the server through a file url
	 *
	 * Takes the provided resource and sends it to the server to perform an
	 * create.
	 *
	 * @param resource	A resource instance
	 * @throws IOException
	 * @returns The Resource as it now exists
	 * @throws A CKANException if the request fails
	 */
	public Resource urlCreateResource(Resource resource) throws CKANException {
		Gson gson = new Gson();
		String data = gson.toJson(resource);
		//System.out.println(data);
		String returned_json = this._connection.Post("/api/action/resource_create", data);
		System.out.println(returned_json);
		Resource.Response r = LoadClass(Resource.Response.class, returned_json);
		if (!r.success) {
			// This will always throw an exception
			HandleError(returned_json, "createResource");
		}
		return r.result;
	}

	/**
	 * Updates a resource on the server through a file upload
	 *
	 * Takes the provided resource and sends it to the server to perform an
	 * update.
	 *
	 * @param resource	A resource instance
	 * @throws IOException
	 * @returns The Resource as it now exists
	 * @throws A CKANException if the request fails
	 */
	public Resource uploadUpdateResource(Resource resource, String filePath) throws CKANException {
		String returned_json = this._connection.ResourceMultiPartPost("/api/action/resource_update", resource, filePath);
		System.out.println(returned_json);
		if (returned_json.lastIndexOf("<html>", 0) != 0) {
			Resource.Response r = LoadClass(Resource.Response.class, returned_json);
			if (!r.success) {
				// This will always throw an exception
				HandleError(returned_json, "updateResource");
			}
			return r.result;
		}
		else {
			HandleHtmlError(returned_json, "updateResource");
			return resource;
		}
	}

	/**
	 * Retrieves a group
	 *
	 * Retrieves the group with the given name, or ID, from the CKAN connection
	 * specified in the Client constructor.
	 *
	 * @param name		The name or ID of the group to fetch
	 * @returns The Group instance for the provided name.
	 * @throws A CKANException if the request fails
	 */
	public Group getGroup(String name) throws CKANException {
		String returned_json = this._connection.Post("/api/action/group_show", "{\"id\":\"" + name + "\"}");
		Group.Response r = LoadClass(Group.Response.class, returned_json);
		if (!r.success) {
			HandleError(returned_json, "getGroup");
		}
		return r.result;
	}

	/**
	 * Deletes a Group
	 *
	 * Deletes the group specified with the provided name/id
	 *
	 * @param name		The name or ID of the group to delete
	 * @throws A CKANException if the request fails
	 */
	public void deleteGroup(String name) throws CKANException {
		String returned_json = this._connection.Post("/api/action/group_delete", "{\"id\":\"" + name + "\"}");
		Group.Response r = LoadClass(Group.Response.class, returned_json);
		if (!r.success) {
			HandleError(returned_json, "deleteGroup");
		}
	}

	/**
	 * Creates a Group on the server
	 *
	 * Takes the provided Group and sends it to the server to perform an create,
	 * and then returns the newly created Group.
	 *
	 * @param group		A Group instance
	 * @returns The Group as it now exists on the server
	 * @throws A CKANException if the request fails
	 */
	public Group createGroup(Group group) throws CKANException {
		Gson gson = new Gson();
		String data = gson.toJson(group);
		String returned_json = this._connection.Post("/api/action/package_create", data);
		Group.Response r = LoadClass(Group.Response.class, returned_json);
		if (!r.success) {
			// This will always throw an exception
			HandleError(returned_json, "createGroup");
		}
		return r.result;
	}

	/**
	 * Uses the provided search term to find datasets on the server
	 *
	 * Takes the provided query and locates those datasets that match the query
	 *
	 * @param query		The search terms
	 * @returns A SearchResults object that contains a count and the objects
	 * @throws A CKANException if the request fails
	 */
	public Dataset.SearchResults findDatasets(String query) throws CKANException {
		
		String returned_json = this._connection.Post("/api/action/package_search", "{\"q\":\"" + query + "\"}");
		Dataset.SearchResponse sr = LoadClass(Dataset.SearchResponse.class, returned_json);
		if (!sr.success) {
			// This will always throw an exception
			HandleError(returned_json, "findDatasets");
		}
		return sr.result;
	}
	
	/**
	 * Creates a datastore table on the server
	 *
	 * Takes the provided datastore table and sends it to the server to perform an
	 * create, and then returns the newly created datastore table.
	 *
	 * @param datastore	A datastore table instance
	 * @returns The datastore table as it now exists
	 * @throws A CKANException if the request fails
	 */
	public DataStore createDataStore(DataStore datastore, int attemptNum) throws CKANException {
		Gson gson = new Gson();
		String data = gson.toJson(datastore);
		String returned_json = this._connection.Post("/api/action/datastore_create", data);
		DataStore.Response r = LoadClass(DataStore.Response.class, returned_json);
		if (!r.success) {
			if (attemptNum > 0){
				attemptNum = attemptNum-1;
				// Sleep awhile before trying again
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					System.out.println( e.toString() );
				}
				HashMap hm = LoadClass(HashMap.class, returned_json);
				Map<String, Object> m = (Map<String, Object>) hm.get("error");
				String errorStatement = "";
				for (Map.Entry<String, Object> entry : m.entrySet()) {
					if (entry.getKey().startsWith("_"))
						continue;
					errorStatement += entry.getValue() + " - " + entry.getKey();
				}
				System.out.println(errorStatement);
				createDataStore(datastore, attemptNum);
			}
			else {
				// This will always throw an exception
				HandleError(returned_json, "createDataStore");
			}
		}
		return r.result;
	}

	/**
	 * Upserts to a datastore table on the server
	 *
	 * Takes the provided datastore table and sends it to the server to perform an
	 * upsert, and then returns the newly updated datastore table.
	 *
	 * @param datastore	A datastore table instance
	 * @returns The DataStore as it now exists
	 * @throws A CKANException if the request fails
	 */
	public DataStore upsertDataStore(DataStore datastore, int attemptNum) throws CKANException {
		Gson gson = new Gson();
		String data = gson.toJson(datastore);
		String returned_json = this._connection.Post("/api/action/datastore_upsert", data);
		DataStore.Response r = LoadClass(DataStore.Response.class, returned_json);
		if (!r.success) {
			if (attemptNum > 0){
				attemptNum = attemptNum-1;
				// Sleep awhile before trying again
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					System.out.println( e.toString() );
				}
				HashMap hm = LoadClass(HashMap.class, returned_json);
				Map<String, Object> m = (Map<String, Object>) hm.get("error");
				String errorStatement = "";
				for (Map.Entry<String, Object> entry : m.entrySet()) {
					if (entry.getKey().startsWith("_"))
						continue;
					errorStatement += entry.getValue() + " - " + entry.getKey();
				}
				System.out.println(errorStatement);
				upsertDataStore(datastore, attemptNum);
			}
			else {
				// This will always throw an exception
				HandleError(returned_json, "upsertDataStore");
			}
		}
		return r.result;
	}
	
}
