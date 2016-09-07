/**
 * Copyright 2016 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-cygnus (FI-WARE project).
 *
 * fiware-cygnus is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-cygnus is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-cygnus. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with iot_support at tid dot es
 */

package com.telefonica.iot.cygnus.sinks;

import org.apache.flume.conf.ConfigurationException;
import com.telefonica.iot.cygnus.backends.ckan.CKANBackendImpl;
import com.telefonica.iot.cygnus.backends.ckan.CKANBackend;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.log.CygnusLogger;
import com.telefonica.iot.cygnus.sinks.Enums.DataModel;
import com.telefonica.iot.cygnus.utils.CommonConstants;
import com.telefonica.iot.cygnus.utils.CommonUtils;
import com.telefonica.iot.cygnus.utils.NGSICharsets;
import com.telefonica.iot.cygnus.utils.NGSIConstants;
import com.telefonica.iot.cygnus.utils.NGSIUtils;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map;
import org.apache.flume.Context;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

/**
 *
 * @author fermin
 *
 * CKAN sink for Orion Context Broker.
 *
 */
public class NGSICKANSink extends NGSISink {

    private static final CygnusLogger logger = new CygnusLogger(NGSICKANSink.class);
    private String apiKey;
    private String ckanHost;
    private String ckanPort;
    private String orionUrl;
    private boolean rowAttrPersistence;
    private boolean ssl;
    private boolean expandJson;
    private CKANBackend persistenceBackend;

    /**
     * Constructor.
     */
    public NGSICKANSink() {
        super();
    } // NGSICKANSink

    /**
     * Gets the CKAN host. It is protected due to it is only required for testing purposes.
     * @return The KCAN host
     */
    protected String getCKANHost() {
        return ckanHost;
    } // getCKANHost

    /**
     * Gets the CKAN port. It is protected due to it is only required for testing purposes.
     * @return The CKAN port
     */
    protected String getCKANPort() {
        return ckanPort;
    } // getCKANPort

    /**
     * Gets the CKAN API key. It is protected due to it is only required for testing purposes.
     * @return The CKAN API key
     */
    protected String getAPIKey() {
        return apiKey;
    } // getAPIKey

    /**
     * Returns if the attribute persistence is row-based. It is protected due to it is only required for testing
     * purposes.
     * @return True if the attribute persistence is row-based, false otherwise
     */
    protected boolean getRowAttrPersistence() {
        return rowAttrPersistence;
    } // getRowAttrPersistence

    /**
     * Returns the persistence backend. It is protected due to it is only required for testing purposes.
     * @return The persistence backend
     */
    protected CKANBackend getPersistenceBackend() {
        return persistenceBackend;
    } // getPersistenceBackend

    /**
     * Sets the persistence backend. It is protected due to it is only required for testing purposes.
     * @param persistenceBackend
     */
    protected void setPersistenceBackend(CKANBackend persistenceBackend) {
        this.persistenceBackend = persistenceBackend;
    } // setPersistenceBackend

    /**
     * Gets if the connections with CKAN is SSL-enabled. It is protected due to it is only required for testing
     * purposes.
     * @return True if the connection is SSL-enabled, false otherwise
     */
    protected boolean getSSL() {
        return this.ssl;
    } // getSSL

    @Override
    public void configure(Context context) {
        apiKey = context.getString("api_key", "nokey");
        logger.debug("Reading configuration (api_key=" + apiKey + ")");
        ckanHost = context.getString("ckan_host", "localhost");
        logger.debug("Reading configuration (ckan_host=" + ckanHost + ")");
        ckanPort = context.getString("ckan_port", "80");
        int intPort = Integer.parseInt(ckanPort);

        if ((intPort <= 0) || (intPort > 65535)) {
            invalidConfiguration = true;
            logger.debug("Invalid configuration (ckan_port=" + ckanPort + ")"
                    + " -- Must be between 0 and 65535");
        } else {
            logger.debug("Reading configuration (ckan_port=" + ckanPort + ")");
        }  // if else

        orionUrl = context.getString("orion_url", "http://localhost:1026");
        logger.debug("Reading configuration (orion_url=" + orionUrl + ")");
        String attrPersistenceStr = context.getString("attr_persistence", "row");
        
        if (attrPersistenceStr.equals("row") || attrPersistenceStr.equals("column")) {
            rowAttrPersistence = attrPersistenceStr.equals("row");
            logger.debug("Attribute persistence set to " + attrPersistenceStr);
        } else {
            throw new ConfigurationException("Attribute persistence must be \"row\" or \"column\" (attr_persistence=" + attrPersistenceStr + ")");
        }  // if else

        String sslStr = context.getString("ssl", "false");
        
        if (sslStr.equals("true") || sslStr.equals("false")) {
            ssl = Boolean.valueOf(sslStr);
            logger.debug("Reading configuration (ssl="
                + sslStr + ")");
        } else  {
            invalidConfiguration = true;
            logger.debug("Invalid configuration (ssl="
                + sslStr + ") -- Must be 'true' or 'false'");
        }  // if else
        
        //TODO: refactor getBoolean
        String expandJsonStr = context.getString("expand_json", "false");
        
        if (expandJsonStr.equals("true") || expandJsonStr.equals("false")) {
            expandJson = Boolean.valueOf(expandJsonStr);
            logger.debug("Reading configuration (expandJson="
                + expandJsonStr + ")");
        } else  {
            invalidConfiguration = true;
            logger.debug("Invalid configuration (expandJson="
                + expandJsonStr + ") -- Must be 'true' or 'false'");
        }  // if else

        super.configure(context);
        
        // Techdebt: allow this sink to work with all the data models
        dataModel = DataModel.DMBYENTITY;
    } // configure

    @Override
    public void start() {
        try {
            persistenceBackend = new CKANBackendImpl(apiKey, ckanHost, ckanPort, orionUrl, ssl);
            logger.debug("CKAN persistence backend created");
        } catch (Exception e) {
            logger.error("Error while creating the CKAN persistence backend. Details=" + e.getMessage());
        } // try catch

        super.start();
    } // start

    @Override
    void persistBatch(NGSIBatch batch) throws Exception {
        if (batch == null) {
            logger.debug("Null batch, nothing to do");
            return;
        } // if
        // iterate on the destinations, for each one a single create / append will be performed
        // TODO: assumes each destination has a uniq organisation and package name; is this true?
        for (String destination : batch.getDestinations()) {
            logger.debug("Processing sub-batch regarding the " + destination
                    + " destination");

            ArrayList<NGSIEvent> subBatch = batch.getEvents(destination);

            getAggregator(subBatch.get(0)).persist(subBatch);

            batch.setPersisted(destination);
        } // for
    } // persistBatch

    /**
     * Class for aggregating fieldValues.
     */
    abstract class Aggregator {

        // string containing the data records
        protected JsonArray records;

        protected CKANBackend backend;
        protected String service;
        protected String servicePath;
        protected String destination;
        protected String orgName;
        protected String pkgName;
        protected String resName;
        protected String resId;
        protected boolean create;

        public Aggregator initialize(String service, String servicePath, String destination, CKANBackend backend, boolean create) throws CygnusBadConfiguration {
            this.records = new JsonArray();
            this.service = service;
            this.servicePath = servicePath;
            this.destination = destination;
            this.backend = backend;
            this.create = create;
            this.orgName = buildOrgName(service).toLowerCase();
            this.pkgName = buildPkgName(service, servicePath).toLowerCase();
            this.resName = buildResName(destination).toLowerCase();
            return this;
        } // initialize

        public String getAggregation() {
            return records.toString();
        } // getAggregation

        public void persist(ArrayList<NGSIEvent> batch) throws Exception {
            for (NGSIEvent cygnusEvent : batch) aggregate(cygnusEvent);

            String aggregation = getAggregation();

            logger.info("Persisting data at OrionCKANSink (orgName=" + orgName + ", pkgName=" +
                pkgName + ", resName=" + resName + ", data=" + aggregation + ")");

            backend.persist(orgName, pkgName, resName, aggregation, create);
        } // persist

        public void aggregate(NGSIEvent event) {
            NotifyContextRequest.ContextElement entity = event.getContextElement();
            String entityId = entity.getId();
            String entityType = entity.getType();
            
            logger.debug("Processing context entity " + entity);

            ArrayList<NotifyContextRequest.ContextAttribute> entityAttrs = entity.getAttributes();
            if (entityAttrs == null || entityAttrs.isEmpty()) {
                logger.warn("No attributes within the notified entity, nothing is done (entity=" + entity + ")");
                return;
            } // if

            long recvTimeTs = event.getRecvTimeTs();
            String recvTime = CommonUtils.getHumanReadable(recvTimeTs, true);
            aggregate(recvTimeTs, recvTime, entityId, entityType, entityAttrs);
        } // aggregate

        public abstract void aggregate(long recvTimeTs, String recvTime, String entityId, String entityType,
          ArrayList<NotifyContextRequest.ContextAttribute> entityAttrs);

    } // Aggregator

    /**
     * Class for aggregating batches in row mode.
     */
    private class RowAggregator extends Aggregator {

        @Override
        public void aggregate(long recvTimeTs, String recvTime, String entityId, String entityType,
          ArrayList<NotifyContextRequest.ContextAttribute> entityAttrs) {
            for (NotifyContextRequest.ContextAttribute contextAttribute : entityAttrs) {
                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                logger.debug("Processing context attribute (name=" + attrName + ", entityType="
                        + attrType + ")");
                String attrMetadata = contextAttribute.getContextMetadata();
                JsonElement attrValue = contextAttribute.getContextValue();

                if (expandJson && attrValue.isJsonObject()) {
                  JsonObject jsonValue = (JsonObject) attrValue;
                  for(Map.Entry<String, JsonElement> entry : jsonValue.entrySet()) {
                    // TODO: refactor record contruction
                    // TODO: probably better use Gson 
                    JsonPrimitive value = (JsonPrimitive) entry.getValue();
                    String type = 
                      value.isString() ? "String" :
                      value.isBoolean() ? "Boolean" :
                      value.isNumber() ? "Number" :
                      "unknown";
                    String name = attrName + "_" + entry.getKey();

                    JsonObject record = new JsonObject();
                    record.addProperty(NGSIConstants.RECV_TIME_TS, String.valueOf(recvTimeTs / 1000));
                    record.addProperty(NGSIConstants.RECV_TIME, recvTime);
                    record.addProperty(NGSIConstants.FIWARE_SERVICE_PATH, servicePath);
                    record.addProperty(NGSIConstants.ENTITY_ID, entityId);
                    record.addProperty(NGSIConstants.ENTITY_TYPE, entityType);
                    record.addProperty(NGSIConstants.ATTR_NAME, name);
                    record.addProperty(NGSIConstants.ATTR_TYPE, type);
                    record.add(NGSIConstants.ATTR_VALUE, value);
                    records.add(record);
                  }
                } else {
                  // create a column and aggregate it
                  JsonObject record = new JsonObject();
                  record.addProperty(NGSIConstants.RECV_TIME_TS, String.valueOf(recvTimeTs / 1000));
                  record.addProperty(NGSIConstants.RECV_TIME, recvTime);
                  record.addProperty(NGSIConstants.FIWARE_SERVICE_PATH, servicePath);
                  record.addProperty(NGSIConstants.ENTITY_ID, entityId);
                  record.addProperty(NGSIConstants.ENTITY_TYPE, entityType);
                  record.addProperty(NGSIConstants.ATTR_NAME, attrName);
                  record.addProperty(NGSIConstants.ATTR_TYPE, attrType);
                  record.add(NGSIConstants.ATTR_VALUE, attrValue);
                  // metadata is an special case, because CKAN doesn't support empty array, e.g. "[ ]"
                  // (http://stackoverflow.com/questions/24207065/inserting-empty-arrays-in-json-type-fields-in-datastore)
                  if (!expandJson && !attrMetadata.equals(CommonConstants.EMPTY_MD)) {
                    record.add(NGSIConstants.ATTR_MD, new JsonParser().parse(attrMetadata));
                  }
                  records.add(record);
                }

                if (expandJson) {
                  // metadata is an special case, because CKAN doesn't support empty array, e.g. "[ ]"
                  // (http://stackoverflow.com/questions/24207065/inserting-empty-arrays-in-json-type-fields-in-datastore)
                  JsonArray mdAttrs = (JsonArray) new JsonParser().parse(attrMetadata);
                  for (JsonElement entry : mdAttrs) {
                    JsonObject mdAttr = (JsonObject) entry;
                    
                    String name = attrName + "_md_" + mdAttr.getAsJsonPrimitive("name").getAsString();
                    String type  = mdAttr.getAsJsonPrimitive("type").getAsString();
                    String value  = mdAttr.getAsJsonPrimitive("value").getAsString();

                    JsonObject record = new JsonObject();
                    record.addProperty(NGSIConstants.RECV_TIME_TS, String.valueOf(recvTimeTs / 1000));
                    record.addProperty(NGSIConstants.RECV_TIME, recvTime);
                    record.addProperty(NGSIConstants.FIWARE_SERVICE_PATH, servicePath);
                    record.addProperty(NGSIConstants.ENTITY_ID, entityId);
                    record.addProperty(NGSIConstants.ENTITY_TYPE, entityType);
                    record.addProperty(NGSIConstants.ATTR_NAME, name);
                    record.addProperty(NGSIConstants.ATTR_TYPE, type);
                    record.addProperty(NGSIConstants.ATTR_VALUE, value);
                    records.add(record);
                  }
                }
            } // for
        } // aggregate

    } // RowAggregator

    /**
     * Class for aggregating batches in column mode.
     */
    private class ColumnAggregator extends Aggregator {

        @Override
        public void aggregate(long recvTimeTs, String recvTime, String entityId, String entityType,
          ArrayList<NotifyContextRequest.ContextAttribute> entityAttrs) {
            JsonObject record = new JsonObject();
            record.addProperty(NGSIConstants.RECV_TIME_TS, String.valueOf(recvTimeTs / 1000));
            record.addProperty(NGSIConstants.RECV_TIME, recvTime);
            record.addProperty(NGSIConstants.FIWARE_SERVICE_PATH, servicePath);
            record.addProperty(NGSIConstants.ENTITY_ID, entityId);
            record.addProperty(NGSIConstants.ENTITY_TYPE, entityType);

            for (NotifyContextRequest.ContextAttribute contextAttribute : entityAttrs) {
                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                JsonElement attrValue = contextAttribute.getContextValue();
                String attrMetadata = contextAttribute.getContextMetadata();
                logger.debug("Processing context attribute (name=" + attrName + ", entityType=" + attrType + ")");

                if (expandJson && attrValue.isJsonObject()) {
                  JsonObject jsonValue = (JsonObject) attrValue;
                  for(Map.Entry<String, JsonElement> entry : jsonValue.entrySet()) {
                    // TODO: refactor record contruction
                    // TODO: probably better use Gson 
                    JsonPrimitive value = (JsonPrimitive) entry.getValue();
                    String name = attrName + "_" + entry.getKey();
                    record.add(name, value);
                  }
                } else {
                  // create part of the column with the current attribute (a.k.a. a column)
                  record.add(attrName, attrValue);
                }

                // metadata is an special case, because CKAN doesn't support empty array, e.g. "[ ]"
                // (http://stackoverflow.com/questions/24207065/inserting-empty-arrays-in-json-type-fields-in-datastore)
                if (expandJson) {
                  JsonArray mdAttrs = (JsonArray) new JsonParser().parse(attrMetadata);
                  for (JsonElement entry : mdAttrs) {
                    JsonObject mdAttr = (JsonObject) entry;
                    
                    String name = attrName + "_md_" + mdAttr.getAsJsonPrimitive("name").getAsString();
                    String value  = mdAttr.getAsJsonPrimitive("value").getAsString();
                    record.addProperty(name, value);
                  }
                } else {
                  if (!attrMetadata.equals(CommonConstants.EMPTY_MD)) {
                      record.add(attrName + "_md", new JsonParser().parse(attrMetadata));
                  } // if
                }
            } // for

            // now, aggregate the column
            records.add(record);
        } // aggregate

    } // ColumnAggregator

    private Aggregator getAggregator(NGSIEvent e) throws CygnusBadConfiguration {
        return (rowAttrPersistence ? new RowAggregator() : new ColumnAggregator())
            .initialize(e.getService(), e.getServicePath(), e.getEntity(), persistenceBackend, rowAttrPersistence);
    } // getAggregator

    private String checkName(String name, String s) throws CygnusBadConfiguration {
        if (s.length() > NGSIConstants.CKAN_MAX_NAME_LEN)
            throw new CygnusBadConfiguration("Length of " + name + " name '" + s + "' exceeds " + NGSIConstants.CKAN_MAX_NAME_LEN);

        if (s.length() < NGSIConstants.CKAN_MIN_NAME_LEN)
            throw new CygnusBadConfiguration("Length of " + name + " name '" + s + "' lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);

        return s;
    }

    /**
     * Builds an organization name given a fiwareService. It throws an exception if the naming conventions are violated.
     *
     *
     * @param fiwareService
     * @return
     * @throws Exception
     */
    public String buildOrgName(String fiwareService) throws CygnusBadConfiguration {
        String orgName = enableEncoding ? NGSICharsets.encodeCKAN(fiwareService) : NGSIUtils.encode(fiwareService, false, true);
        return checkName("organization", orgName);
    } // buildOrgName

    /**
     * Builds a package name given a fiwareService and a fiwareServicePath. It throws an exception if the naming
     * conventions are violated.
     * @param fiwareService
     * @param fiwareServicePath
     * @return
     * @throws Exception
     */
    public String buildPkgName(String fiwareService, String fiwareServicePath) throws CygnusBadConfiguration {
        String pkgName = enableEncoding ? NGSICharsets.encodeCKAN(fiwareService) + NGSICharsets.encodeCKAN(fiwareServicePath) :
          fiwareServicePath.equals("/") ? NGSIUtils.encode(fiwareService, false, true) :
            NGSIUtils.encode(fiwareService, false, true) + NGSIUtils.encode(fiwareServicePath, false, true);
        return checkName("package", pkgName);
    } // buildPkgName

    /**
     * Builds a resource name given a destination. It throws an exception if the naming conventions are violated.
     * @param destination
     * @return
     * @throws Exception
     */
    public String buildResName(String destination) throws CygnusBadConfiguration {
        String resName = enableEncoding ? NGSICharsets.encodeCKAN(destination) :NGSIUtils.encode(destination, false, true);
        return checkName("resource", resName);
    } // buildResName


} // NGSICKANSink
