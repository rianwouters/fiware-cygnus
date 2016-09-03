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

/**
 *
 * @author fermin
 *
 * CKAN sink for Orion Context Broker.
 *
 */
public class NGSICKANSink extends NGSISink {

    private static final CygnusLogger LOGGER = new CygnusLogger(NGSICKANSink.class);
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
        LOGGER.debug("[" + this.getName() + "] Reading configuration (api_key=" + apiKey + ")");
        ckanHost = context.getString("ckan_host", "localhost");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (ckan_host=" + ckanHost + ")");
        ckanPort = context.getString("ckan_port", "80");
        int intPort = Integer.parseInt(ckanPort);

        if ((intPort <= 0) || (intPort > 65535)) {
            invalidConfiguration = true;
            LOGGER.debug("[" + this.getName() + "] Invalid configuration (ckan_port=" + ckanPort + ")"
                    + " -- Must be between 0 and 65535");
        } else {
            LOGGER.debug("[" + this.getName() + "] Reading configuration (ckan_port=" + ckanPort + ")");
        }  // if else

        orionUrl = context.getString("orion_url", "http://localhost:1026");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (orion_url=" + orionUrl + ")");
        String attrPersistenceStr = context.getString("attr_persistence", "row");
        
        if (attrPersistenceStr.equals("row") || attrPersistenceStr.equals("column")) {
            rowAttrPersistence = attrPersistenceStr.equals("row");
            LOGGER.debug("[" + this.getName() + "] Reading configuration (attr_persistence="
                + attrPersistenceStr + ")");
        } else {
            invalidConfiguration = true;
            LOGGER.debug("[" + this.getName() + "] Invalid configuration (attr_persistence="
                + attrPersistenceStr + ") -- Must be 'row' or 'column'");
        }  // if else

        String sslStr = context.getString("ssl", "false");
        
        if (sslStr.equals("true") || sslStr.equals("false")) {
            ssl = Boolean.valueOf(sslStr);
            LOGGER.debug("[" + this.getName() + "] Reading configuration (ssl="
                + sslStr + ")");
        } else  {
            invalidConfiguration = true;
            LOGGER.debug("[" + this.getName() + "] Invalid configuration (ssl="
                + sslStr + ") -- Must be 'true' or 'false'");
        }  // if else
        
        //TODO: refactor getBoolean
        String expandJsonStr = context.getString("expandJson", "false");
        
        if (expandJsonStr.equals("true") || expandJsonStr.equals("false")) {
            expandJson = Boolean.valueOf(expandJsonStr);
            LOGGER.debug("[" + this.getName() + "] Reading configuration (expandJson="
                + expandJsonStr + ")");
        } else  {
            invalidConfiguration = true;
            LOGGER.debug("[" + this.getName() + "] Invalid configuration (expandJson="
                + expandJsonStr + ") -- Must be 'true' or 'false'");
        }  // if else

        super.configure(context);
        
        // Techdebt: allow this sink to work with all the data models
        dataModel = DataModel.DMBYENTITY;
    
        // CKAN requires all the names written in lower case
        enableLowercase = true;
    } // configure

    @Override
    public void start() {
        try {
            persistenceBackend = new CKANBackendImpl(apiKey, ckanHost, ckanPort, orionUrl, ssl);
            LOGGER.debug("[" + this.getName() + "] CKAN persistence backend created");
        } catch (Exception e) {
            LOGGER.error("Error while creating the CKAN persistence backend. Details="
                    + e.getMessage());
        } // try catch

        super.start();
    } // start

    @Override
    void persistBatch(NGSIBatch batch) throws Exception {
        if (batch == null) {
            LOGGER.debug("[" + this.getName() + "] Null batch, nothing to do");
            return;
        } // if

        // iterate on the destinations, for each one a single create / append will be performed
        for (String destination : batch.getDestinations()) {
            LOGGER.debug("[" + this.getName() + "] Processing sub-batch regarding the " + destination
                    + " destination");

            // get the sub-batch for this destination
            ArrayList<NGSIEvent> subBatch = batch.getEvents(destination);

            // get an aggregator for this destination and initialize it
            CKANAggregator aggregator = getAggregator(this.rowAttrPersistence);
            aggregator.initialize(subBatch.get(0));

            for (NGSIEvent cygnusEvent : subBatch) {
                aggregator.aggregate(cygnusEvent);
            } // for

            // persist the aggregation
            persistAggregation(aggregator);
            batch.setPersisted(destination);
        } // for
    } // persistBatch

    /**
     * Class for aggregating fieldValues.
     */
    private abstract class CKANAggregator {

        // string containing the data records
        protected List<String> records;

        protected String service;
        protected String servicePath;
        protected String destination;
        protected String orgName;
        protected String pkgName;
        protected String resName;
        protected String resId;

        public CKANAggregator() {
            records = new ArrayList<String>();
        } // CKANAggregator

        public String getAggregation() {
            return String.join(",", records);
        } // getAggregation

        public String getOrgName(boolean enableLowercase) {
            if (enableLowercase) {
                return orgName.toLowerCase();
            } else {
                return orgName;
            } // if else
        } // getOrgName

        public String getPkgName(boolean enableLowercase) {
            if (enableLowercase) {
                return pkgName.toLowerCase();
            } else {
                return pkgName;
            } // if else
        } // getPkgName

        public String getResName(boolean enableLowercase) {
            if (enableLowercase) {
                return resName.toLowerCase();
            } else {
                return resName;
            } // if else
        } // getResName

        public void initialize(NGSIEvent cygnusEvent) throws Exception {
            service = cygnusEvent.getService();
            servicePath = cygnusEvent.getServicePath();
            destination = cygnusEvent.getEntity();
            orgName = buildOrgName(service);
            pkgName = buildPkgName(service, servicePath);
            resName = buildResName(destination);
        } // initialize

        public abstract void aggregate(NGSIEvent cygnusEvent) throws Exception;

    } // CKANAggregator

    /**
     * Class for aggregating batches in row mode.
     */
    private class RowAggregator extends CKANAggregator {

        @Override
        public void initialize(NGSIEvent cygnusEvent) throws Exception {
            super.initialize(cygnusEvent);
        } // initialize

        @Override
        public void aggregate(NGSIEvent cygnusEvent) throws Exception {
            // get the event headers
            long recvTimeTs = cygnusEvent.getRecvTimeTs();
            String recvTime = CommonUtils.getHumanReadable(recvTimeTs, true);

            // get the event body
            NotifyContextRequest.ContextElement contextElement = cygnusEvent.getContextElement();
            String entityId = contextElement.getId();
            String entityType = contextElement.getType();
            LOGGER.debug("[" + getName() + "] Processing context element (id=" + entityId + ", type="
                    + entityType + ")");

            // iterate on all this context element attributes, if there are attributes
            ArrayList<NotifyContextRequest.ContextAttribute> contextAttributes = contextElement.getAttributes();

            if (contextAttributes == null || contextAttributes.isEmpty()) {
                LOGGER.warn("No attributes within the notified entity, nothing is done (id=" + entityId
                        + ", type=" + entityType + ")");
                return;
            } // if

            for (NotifyContextRequest.ContextAttribute contextAttribute : contextAttributes) {
                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                LOGGER.debug("[" + getName() + "] Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");
                String attrMetadata = contextAttribute.getContextMetadata();
                JsonElement attrValue = contextAttribute.getContextValue();

                // metadata is an special case, because CKAN doesn't support empty array, e.g. "[ ]"
                // (http://stackoverflow.com/questions/24207065/inserting-empty-arrays-in-json-type-fields-in-datastore)
                String metadataStr = attrMetadata.equals(CommonConstants.EMPTY_MD) ? "" : ",\"" + NGSIConstants.ATTR_MD + "\": " + attrMetadata;

                if (expandJson && attrValue.isJsonObject()) {
                  JsonObject jsonValue = (JsonObject) attrValue;
                  for(Map.Entry<String, JsonElement> entry : jsonValue.entrySet()) {
                    // TODO: refactor record contruction
                    
                    JsonPrimitive value = (JsonPrimitive) entry.getValue();
                    String type = 
                      value.isString() ? "String" :
                      value.isBoolean() ? "Boolean" :
                      value.isNumber() ? "Number" :
                      "unknown";
                    String name = attrName + "_" + entry.getKey(); 
                    String record = "{\"" + NGSIConstants.RECV_TIME_TS + "\": \"" + recvTimeTs / 1000 + "\","
                        + "\"" + NGSIConstants.RECV_TIME + "\": \"" + recvTime + "\","
                        + "\"" + NGSIConstants.FIWARE_SERVICE_PATH + "\": \"" + servicePath + "\","
                        + "\"" + NGSIConstants.ENTITY_ID + "\": \"" + entityId + "\","
                        + "\"" + NGSIConstants.ENTITY_TYPE + "\": \"" + entityType + "\","
                        + "\"" + NGSIConstants.ATTR_NAME + "\": \"" + name + "\","
                        + "\"" + NGSIConstants.ATTR_TYPE + "\": \"" + type + "\","
                        + "\"" + NGSIConstants.ATTR_VALUE + "\": " + value + metadataStr + "}";
                    records.add(record);
                  }
                } else {
                  // create a column and aggregate it
                  String record = "{\"" + NGSIConstants.RECV_TIME_TS + "\": \"" + recvTimeTs / 1000 + "\","
                      + "\"" + NGSIConstants.RECV_TIME + "\": \"" + recvTime + "\","
                      + "\"" + NGSIConstants.FIWARE_SERVICE_PATH + "\": \"" + servicePath + "\","
                      + "\"" + NGSIConstants.ENTITY_ID + "\": \"" + entityId + "\","
                      + "\"" + NGSIConstants.ENTITY_TYPE + "\": \"" + entityType + "\","
                      + "\"" + NGSIConstants.ATTR_NAME + "\": \"" + attrName + "\","
                      + "\"" + NGSIConstants.ATTR_TYPE + "\": \"" + attrType + "\","
                      + "\"" + NGSIConstants.ATTR_VALUE + "\": " + attrValue.toString() + metadataStr + "}";
                  records.add(record);
                }

            } // for
        } // aggregate

    } // RowAggregator

    /**
     * Class for aggregating batches in column mode.
     */
    private class ColumnAggregator extends CKANAggregator {

        @Override
        public void initialize(NGSIEvent cygnusEvent) throws Exception {
            super.initialize(cygnusEvent);
        } // initialize

        @Override
        public void aggregate(NGSIEvent cygnusEvent) throws Exception {
            // get the event headers
            long recvTimeTs = cygnusEvent.getRecvTimeTs();
            String recvTime = CommonUtils.getHumanReadable(recvTimeTs, true);

            // get the event body
            NotifyContextRequest.ContextElement contextElement = cygnusEvent.getContextElement();
            String entityId = contextElement.getId();
            String entityType = contextElement.getType();
            LOGGER.debug("[" + getName() + "] Processing context element (id=" + entityId + ", type="
                    + entityType + ")");

            // iterate on all this context element attributes, if there are attributes
            ArrayList<NotifyContextRequest.ContextAttribute> contextAttributes = contextElement.getAttributes();

            if (contextAttributes == null || contextAttributes.isEmpty()) {
                LOGGER.warn("No attributes within the notified entity, nothing is done (id=" + entityId
                        + ", type=" + entityType + ")");
                return;
            } // if

            String record = "{\"" + NGSIConstants.RECV_TIME + "\": \"" + recvTime + "\","
                    + "\"" + NGSIConstants.FIWARE_SERVICE_PATH + "\": \"" + servicePath + "\","
                    + "\"" + NGSIConstants.ENTITY_ID + "\": \"" + entityId + "\","
                    + "\"" + NGSIConstants.ENTITY_TYPE + "\": \"" + entityType + "\"";

            for (NotifyContextRequest.ContextAttribute contextAttribute : contextAttributes) {
                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                String attrValue = contextAttribute.getContextValue(true);
                String attrMetadata = contextAttribute.getContextMetadata();
                LOGGER.debug("[" + getName() + "] Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");

                // create part of the column with the current attribute (a.k.a. a column)
                record += ",\"" + attrName + "\": " + attrValue;

                // metadata is an special case, because CKAN doesn't support empty array, e.g. "[ ]"
                // (http://stackoverflow.com/questions/24207065/inserting-empty-arrays-in-json-type-fields-in-datastore)
                if (!attrMetadata.equals(CommonConstants.EMPTY_MD)) {
                    record += ",\"" + attrName + "_md\": " + attrMetadata;
                } // if
            } // for

            // now, aggregate the column
            records.add(record);
        } // aggregate

    } // ColumnAggregator

    private CKANAggregator getAggregator(boolean rowAttrPersistence) {
        if (rowAttrPersistence) {
            return new RowAggregator();
        } else {
            return new ColumnAggregator();
        } // if else
    } // getAggregator

    private void persistAggregation(CKANAggregator aggregator) throws Exception {
        String aggregation = aggregator.getAggregation();
        String orgName = aggregator.getOrgName(enableLowercase);
        String pkgName = aggregator.getPkgName(enableLowercase);
        String resName = aggregator.getResName(enableLowercase);

        LOGGER.info("[" + this.getName() + "] Persisting data at OrionCKANSink (orgName=" + orgName
                + ", pkgName=" + pkgName + ", resName=" + resName + ", data=" + aggregation + ")");

        persistenceBackend.persist(orgName, pkgName, resName, aggregation, aggregator instanceof RowAggregator);
    } // persistAggregation

    /**
     * Builds an organization name given a fiwareService. It throws an exception if the naming conventions are violated.
     * @param fiwareService
     * @return
     * @throws Exception
     */
    public String buildOrgName(String fiwareService) throws Exception {
        String orgName;
        
        if (enableEncoding) {
            orgName = NGSICharsets.encodeCKAN(fiwareService);
        } else {
            orgName = NGSIUtils.encode(fiwareService, false, true);
        } // if else

        if (orgName.length() > NGSIConstants.CKAN_MAX_NAME_LEN) {
            throw new CygnusBadConfiguration("Building organization name '" + orgName + "' and its length is "
                    + "greater than " + NGSIConstants.CKAN_MAX_NAME_LEN);
        } else if (orgName.length() < NGSIConstants.CKAN_MIN_NAME_LEN) {
            throw new CygnusBadConfiguration("Building organization name '" + orgName + "' and its length is "
                    + "lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);
        } // if else if
            
        return orgName;
    } // buildOrgName

    /**
     * Builds a package name given a fiwareService and a fiwareServicePath. It throws an exception if the naming
     * conventions are violated.
     * @param fiwareService
     * @param fiwareServicePath
     * @return
     * @throws Exception
     */
    public String buildPkgName(String fiwareService, String fiwareServicePath) throws Exception {
        String pkgName;
        
        if (enableEncoding) {
            pkgName = NGSICharsets.encodeCKAN(fiwareService) + NGSICharsets.encodeCKAN(fiwareServicePath);
        } else {
            if (fiwareServicePath.equals("/")) {
                pkgName = NGSIUtils.encode(fiwareService, false, true);
            } else {
                pkgName = NGSIUtils.encode(fiwareService, false, true)
                        + NGSIUtils.encode(fiwareServicePath, false, true);
            } // if else
        } // if else

        if (pkgName.length() > NGSIConstants.CKAN_MAX_NAME_LEN) {
            throw new CygnusBadConfiguration("Building package name '" + pkgName + "' and its length is "
                    + "greater than " + NGSIConstants.CKAN_MAX_NAME_LEN);
        } else if (pkgName.length() < NGSIConstants.CKAN_MIN_NAME_LEN) {
            throw new CygnusBadConfiguration("Building package name '" + pkgName + "' and its length is "
                    + "lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);
        } // if else if

        return pkgName;
    } // buildPkgName

    /**
     * Builds a resource name given a destination. It throws an exception if the naming conventions are violated.
     * @param destination
     * @return
     * @throws Exception
     */
    public String buildResName(String destination) throws Exception {
        String resName;
        
        if (enableEncoding) {
            resName = NGSICharsets.encodeCKAN(destination);
        } else {
            resName = NGSIUtils.encode(destination, false, true);
        } // if else

        if (resName.length() > NGSIConstants.CKAN_MAX_NAME_LEN) {
            throw new CygnusBadConfiguration("Building resource name '" + resName + "' and its length is "
                    + "greater than " + NGSIConstants.CKAN_MAX_NAME_LEN);
        } else if (resName.length() < NGSIConstants.CKAN_MIN_NAME_LEN) {
            throw new CygnusBadConfiguration("Building resource name '" + resName + "' and its length is "
                    + "lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);
        } // if else if

        return resName;
    } // buildResName

} // NGSICKANSink
