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

import com.amazonaws.services.dynamodbv2.document.Item;
import com.telefonica.iot.cygnus.backends.dynamo.DynamoDBBackend;
import com.telefonica.iot.cygnus.backends.dynamo.DynamoDBBackendImpl;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.log.CygnusLogger;
import com.telefonica.iot.cygnus.sinks.Enums.DataModel;
import static com.telefonica.iot.cygnus.sinks.Enums.DataModel.DMBYENTITY;
import static com.telefonica.iot.cygnus.sinks.Enums.DataModel.DMBYSERVICEPATH;
import com.telefonica.iot.cygnus.utils.CommonConstants;
import com.telefonica.iot.cygnus.utils.CommonUtils;
import com.telefonica.iot.cygnus.utils.NGSIConstants;
import com.telefonica.iot.cygnus.utils.NGSIUtils;
import java.util.ArrayList;
import java.util.Date;
import org.apache.flume.Context;

/**
 *
 * @author frb
 */
public class NGSIDynamoDBSink extends NGSISink {
    
    /**
     * Available DynamoDB regions implementation.
     */
    private enum Regions { USEAST1, USWEST1, USWEST2,  EUWEST1, EUCENTRAL1,
            APNORTHEAST1, APNORTHEAST2, APSHOUTEAST1, APSHOUTEAST2, SAEAST1 }

    private static final CygnusLogger LOGGER = new CygnusLogger(NGSIDynamoDBSink.class);
    private DynamoDBBackend persistenceBackend;
    private String accessKeyId;
    private String secretAccessKey;
    private String region;
    private boolean attrPersistenceRow;
    private long id = new Date().getTime();

    protected DynamoDBBackend getPersistenceBackend() {
        return persistenceBackend;
    } // getPersistenceBackend

    protected void setPersistenceBackend(DynamoDBBackend persistenceBackend) {
        this.persistenceBackend = persistenceBackend;
    } // setPersistenceBackend

    protected String getAccessKeyId() {
        return accessKeyId;
    } // getAccessKeyId

    protected String getSecretAccessKey() {
        return secretAccessKey;
    } // getSecretAccessKey

    protected String getRegion() {
        return region;
    } // getRegion

    protected boolean getRowAttrPersistence() {
        return attrPersistenceRow;
    } // getRowAttrPersistence

    @Override
    public void configure(Context context) {
        accessKeyId = context.getString("access_key_id");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (access_key_id=" + accessKeyId + ")");
        secretAccessKey = context.getString("secret_access_key");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (secret_access_key=" + secretAccessKey + ")");
        
        String regionStr = context.getString("region", "eu-west-1");

        try {
            Regions regionReg = Regions.valueOf(regionStr.replaceAll("-", "").toUpperCase());
            region = regionStr;
            LOGGER.debug("[" + this.getName() + "] Reading configuration (region="
                    + regionStr + ")");
        } catch (Exception e) {
            invalidConfiguration = true;
            LOGGER.debug("[" + this.getName() + "] Invalid configuration (region="
                    + regionStr + ") -- Must be 'us-east-1', 'us-west-1', 'us-west-2',  'eu-west-1', 'eu-central-1', "
                    + "'ap-northeast-1', 'ap-northeast1', 'ap-shouteast-1', 'ap-shouteast-2' or 'sa-east-1'");
        } // catch
        
        String attrPersistRowStr = context.getString("attr_persistence", "row");
        
        if (attrPersistRowStr.equals("row") || attrPersistRowStr.equals("column")) {
            attrPersistenceRow = attrPersistRowStr.equals("row");
            LOGGER.debug("[" + this.getName() + "] Reading configuration (attr_persistence="
                + attrPersistRowStr + ")");
        } else {
            invalidConfiguration = true;
            LOGGER.debug("[" + this.getName() + "] Invalid configuration (attr_persistence="
                + attrPersistRowStr + ") -- Must be 'row' or 'column'");
        }  // if else

        super.configure(context);
    } // configure

    @Override
    public void start() {
        try {
            persistenceBackend = new DynamoDBBackendImpl(accessKeyId, secretAccessKey, region);
            LOGGER.debug("[" + this.getName() + "] DynamoDB persistence backend created");
        } catch (Exception e) {
            LOGGER.error("Error while creating the DynamoDB persistence backend. Details="
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
            DynamoDBAggregator aggregator = getAggregator(attrPersistenceRow);
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
     * Class for aggregating data regarding a destination in a servicePath, in a service.
     */
    private abstract class DynamoDBAggregator {

        // string containing the data aggregation
        protected ArrayList<Item> aggregation;
        protected String service;
        protected String servicePath;
        protected String destination;
        protected String tableName;

        public ArrayList<Item> getAggregation() {
            return aggregation;
        } // getAggregation

        public String getTableName(boolean enableLowercase) {
            if (enableLowercase) {
                return tableName.toLowerCase();
            } else {
                return tableName;
            } // if else
        } // getTableName

        public void initialize(NGSIEvent cygnusEvent) throws Exception {
            service = cygnusEvent.getService();
            servicePath = cygnusEvent.getServicePath();
            destination = cygnusEvent.getEntity();
            tableName = buildTableName(service, servicePath, destination, dataModel);
            aggregation = new ArrayList<Item>();
        } // initialize

        public abstract void aggregate(NGSIEvent cygnusEvent) throws Exception;

    } // DynamoDBAggregator

    /**
     * Class for aggregating batches in row mode.
     */
    private class DynamoDBRowAggregator extends DynamoDBAggregator {

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
                String attrValue = contextAttribute.getContextValue(false);
                String attrMetadata = contextAttribute.getContextMetadata();
                LOGGER.debug("[" + getName() + "] Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");

                // create an item and aggregate it
                Item item = new Item()
                        .withPrimaryKey(NGSIConstants.DYNAMO_DB_PRIMARY_KEY, id)
                        .withDouble(NGSIConstants.RECV_TIME_TS, recvTimeTs / 1000)
                        .withString(NGSIConstants.RECV_TIME, recvTime)
                        .withString(NGSIConstants.FIWARE_SERVICE_PATH, servicePath)
                        .withString(NGSIConstants.ENTITY_ID, entityId)
                        .withString(NGSIConstants.ENTITY_TYPE, entityType)
                        .withString(NGSIConstants.ATTR_NAME, attrName)
                        .withString(NGSIConstants.ATTR_TYPE, attrType)
                        .withString(NGSIConstants.ATTR_VALUE, attrValue)
                        .withString(NGSIConstants.ATTR_MD, attrMetadata);
                aggregation.add(item);

                // id count update
                id++;
            } // for
        } // aggregate

    } // DynamoDBRowAggregator

    /**
     * Class for aggregating batches in column mode.
     */
    private class DynamoDBColumnAggregator extends DynamoDBAggregator {

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

            // create an item
            Item item = new Item()
                    .withPrimaryKey(NGSIConstants.DYNAMO_DB_PRIMARY_KEY, id)
                    .withString(NGSIConstants.RECV_TIME, recvTime)
                    .withString(NGSIConstants.FIWARE_SERVICE_PATH, servicePath)
                    .withString(NGSIConstants.ENTITY_ID, entityId)
                    .withString(NGSIConstants.ENTITY_TYPE, entityType);

            for (NotifyContextRequest.ContextAttribute contextAttribute : contextAttributes) {
                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                String attrValue = contextAttribute.getContextValue(true);
                String attrMetadata = contextAttribute.getContextMetadata();
                LOGGER.debug("[" + getName() + "] Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");

                item.withString(attrName, attrValue)
                        .withString(attrName + "_md", attrMetadata);
            } // for

            // aggregate the item
            aggregation.add(item);

            // id count update
            id++;
        } // aggregate

    } // DynamoDBColumnAggregator

    private DynamoDBAggregator getAggregator(boolean attrPersistenceRow) {
        if (attrPersistenceRow) {
            return new DynamoDBRowAggregator();
        } else {
            return new DynamoDBColumnAggregator();
        } // if else
    } // getAggregator

    private void persistAggregation(DynamoDBAggregator aggregator) throws Exception {
        ArrayList aggregation = aggregator.getAggregation();
        String tableName = aggregator.getTableName(enableLowercase);

        LOGGER.info("[" + this.getName() + "] Persisting data at OrionDynamoSink. Dynamo table ("
                + tableName + "), Data (" + aggregation.toString() + ")");

        // tables can be always created in DynamoDB, independedntly of the attribute persistence mode,
        // since it is NoSQL and there is no fixed structure
        persistenceBackend.createTable(tableName, NGSIConstants.DYNAMO_DB_PRIMARY_KEY);
        persistenceBackend.putItems(tableName, aggregation);
    } // persistAggregation

    private String buildTableName(String service, String servicePath, String destination, DataModel dataModel)
        throws Exception {
        String tableName;

        switch (dataModel) {
            case DMBYSERVICEPATH:
                String truncatedServicePath = NGSIUtils.encode(servicePath, true, false);
                tableName = NGSIUtils.encode(service, false, true)
                        + (truncatedServicePath.isEmpty() ? "" : "_" + truncatedServicePath);
                break;
            case DMBYENTITY:
                truncatedServicePath = NGSIUtils.encode(servicePath, true, false);
                tableName = NGSIUtils.encode(service, false, true)
                        + (truncatedServicePath.isEmpty() ? "" : "_" + truncatedServicePath)
                        + "_" + NGSIUtils.encode(destination, false, true);
                break;
            default:
                throw new CygnusBadConfiguration("Unknown data model '" + dataModel.toString()
                            + "'. Please, use DMBYSERVICEPATH or DMBYENTITY");
        } // switch

        if (tableName.length() > CommonConstants.MAX_NAME_LEN) {
            throw new CygnusBadConfiguration("Building tableName '" + tableName
                    + "' and its length is greater than " + CommonConstants.MAX_NAME_LEN);
        } // if

        return tableName;
    } // buildTableName

} // NGSIDynamoDBSink
