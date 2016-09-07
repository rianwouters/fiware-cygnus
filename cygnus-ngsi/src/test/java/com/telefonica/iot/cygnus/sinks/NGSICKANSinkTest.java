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

import static org.junit.Assert.*; // this is required by "fail" like assertions
import org.apache.flume.conf.ConfigurationException;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest;
import com.telefonica.iot.cygnus.backends.ckan.CKANBackend;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import static com.telefonica.iot.cygnus.utils.CommonUtilsForTests.getTestTraceHead;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.apache.flume.Context;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Test;
import java.util.ArrayList;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;


/**
 *
 * @author fgalan
 */
@RunWith(MockitoJUnitRunner.class)
public class NGSICKANSinkTest {
    
    /**
     * Constructor.
     */
    public NGSICKANSinkTest() {
        LogManager.getRootLogger().setLevel(Level.FATAL);
    } // NGSICKANSinkTest

    /**
     * [NGSICKANSink.configure] -------- enable_encoding can only be 'true' or 'false'.
     */
    @Test
    public void testConfigureEnableEncoding() {
        System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                + "-------- enable_encoding can only be 'true' or 'false'");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = "falso";
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "-  OK  - 'enable_encoding=falso' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "- FAIL - 'enable_encoding=falso' was not detected");
            throw e;
        } // try catch
    } // testConfigureEnableEncoding
    
    /**
     * [NGSICKANSink.configure] -------- enable_lowercase can only be 'true' or 'false'.
     */
    @Test
    public void testConfigureEnableLowercase() {
        System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                + "-------- enable_lowercase can only be 'true' or 'false'");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = null; // default
        String enableGrouping = null; // default
        String enableLowercase = "falso";
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "-  OK  - 'enable_lowercase=falso' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "- FAIL - 'enable_lowercase=falso' was not detected");
            throw e;
        } // try catch
    } // testConfigureEnableLowercase
    
    /**
     * [NGSICKANSink.configure] -------- enable_grouping can only be 'true' or 'false'.
     */
    @Test
    public void testConfigureEnableGrouping() {
        System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                + "-------- enable_grouping can only be 'true' or 'false'");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = null; // default
        String enableGrouping = "falso";
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "-  OK  - 'enable_grouping=falso' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "- FAIL - 'enable_grouping=falso' was not detected");
            throw e;
        } // try catch
    } // testConfigureEnableGrouping
    
    /**
     * [NGSICKANSink.configure] -------- data_model can only be 'dm-by-entity'.
     */
    // TBD: check for dataModel values in NGSIMySQLSink and uncomment this test.
    // @Test
    public void testConfigureDataModel() {
        System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                + "-------- data_model can only be 'dm-by-entity'");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = "dm-by-service";
        String enableEncoding = null; // default
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "-  OK  - 'data_model=dm-by-service' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "- FAIL - 'data_model=dm-by-service' was not detected");
            throw e;
        } // try catch
    } // testConfigureDataModel
    
    /**
     * [NGSICKANSink.configure] -------- attr_persistence can only be 'row' or 'column'.
     */
    @Test
    public void testConfigureAttrPersistence() {
        System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                + "-------- attr_persistence can only be 'row' or 'column'");
        String attrPersistence = "fila";
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = null; // default
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        
        try {
            sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                    enableGrouping, enableLowercase, host, password, port, username));
            assertFalse(true);
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "-  FAIL  - 'attr_persistence=fila' admitted");
        } catch (ConfigurationException e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.configure]")
                    + "-  OK  - 'attr_persistence=fila' was detected");
            assertTrue(true);
        } // try catch
    } // testConfigureAttrPersistence
    
    /**
     * [NGSICKANSink.buildOrgName] -------- When no encoding, the org name is equals to the encoding of the
     * notified/defaulted service.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildOrgNameNoEncoding() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                + "-------- When no encoding, the org name is equals to the encoding of the notified/defaulted "
                + "service");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = "false";
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String service = "someService";
        
        try {
            String builtOrgName = sink.buildOrgName(service);
            String expectedOrgName = "someService";
        
            try {
                assertEquals(expectedOrgName, builtOrgName);
                System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                        + "-  OK  - '" + expectedOrgName + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                        + "- FAIL - '" + expectedOrgName + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildOrgNameNoEncoding
    
    /**
     * [NGSICKANSink.buildOrgName] -------- When encoding, the org name is equals to the encoding of the
     * notified/defaulted service.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildOrgNameEncoding() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                + "-------- When encoding, the org name is equals to the encoding of the notified/defaulted service");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = "true";
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String service = "someService";
        
        try {
            String builtOrgName = sink.buildOrgName(service);
            String expectedOrgName = "somex0053ervice";
        
            try {
                assertEquals(expectedOrgName, builtOrgName);
                System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                        + "-  OK  - '" + expectedOrgName + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                        + "- FAIL - '" + expectedOrgName + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildOrgNameEncoding
    
    /**
     * [NGSICKANSink.buildPkgName] -------- When no encoding and when using a notified/defaulted non root service path,
     * the pkg name is equals to the encoding of the concatenation of the notified/defaulted service and the
     * notified/defaulted service path.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildPkgNameNonRootServicePathNoEncoding() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                + "-------- When no encoding and when using a notified/defaulted non root service path, the pkg name "
                + "is equals to the encoding of the concatenation of the notified/defaulted service and the "
                + "notified/defaulted service path");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = "false";
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String service = "someService";
        String servicePath = "/someServicePath";
        
        try {
            String builtPkgName = sink.buildPkgName(service, servicePath);
            String expectedPkgName = "someService_someServicePath";
        
            try {
                assertEquals(expectedPkgName, builtPkgName);
                System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                        + "-  OK  - '" + expectedPkgName + "' is equals to the encoding of "
                        + "<service>xffff<servicePath>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                        + "- FAIL - '" + expectedPkgName + "' is not equals to the encoding of "
                        + "<service>xffff<servicePath>");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildPkgNameNonRootServicePathNoEncoding
    
    /**
     * [NGSICKANSink.buildPkgName] -------- When encoding and when using a notified/defaulted non root service path,
     * the pkg name is equals to the encoding of the concatenation of the notified/defaulted service and the
     * notified/defaulted service path.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildPkgNameNonRootServicePathEncoding() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                + "-------- When encoding and when using a notified/defaulted non root service path, the pkg name is "
                + "equals to the encoding of the concatenation of the notified/defaulted service and the "
                + "notified/defaulted service path");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = "true";
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String service = "someService";
        String servicePath = "/someServicePath";
        
        try {
            String builtPkgName = sink.buildPkgName(service, servicePath);
            String expectedPkgName = "somex0053ervicex002fsomex0053ervicex0050ath";
        
            try {
                assertEquals(expectedPkgName, builtPkgName);
                System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                        + "-  OK  - '" + expectedPkgName + "' is equals to the encoding of "
                        + "<service>xffff<servicePath>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                        + "- FAIL - '" + expectedPkgName + "' is not equals to the encoding of "
                        + "<service>xffff<servicePath>");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildPkgNameNonRootServicePathEncoding
    
    /**
     * [NGSICKANSink.buildPkgName] -------- When no encoding and when using a notified/defaulted root service path, the
     * pkg name is equals to the encoding of the concatenation of the notified/defaulted service and the
     * notified/defaulted service path.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildPkgNameRootServicePathNoEncoding() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                + "-------- When no encoding and when using a notified/defaulted root service path, the pkg name is "
                + "equals to the encoding of the concatenation of the notified/defaulted service and the "
                + "notified/defaulted service path");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = "false";
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String service = "someService";
        String servicePath = "/";
        
        try {
            String builtPkgName = sink.buildPkgName(service, servicePath);
            String expectedPkgName = "someService";
        
            try {
                assertEquals(expectedPkgName, builtPkgName);
                System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                        + "-  OK  - '" + expectedPkgName + "' is equals to the encoding of "
                        + "<service>xffff<servicePath>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                        + "- FAIL - '" + expectedPkgName + "' is not equals to the encoding of "
                        + "<service>xffff<servicePath>");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildPkgNameRootServicePathNoEncoding
    
    /**
     * [NGSICKANSink.buildPkgName] -------- When encoding and when using a notified/defaulted root service path, the
     * pkg name is equals to the encoding of the concatenation of the notified/defaulted service and the
     * notified/defaulted service path.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildPkgNameRootServicePathEncoding() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                + "-------- When encoding and when using a notified/defaulted root service path, the pkg name "
                + "equals the encoding of the concatenation of the notified/defaulted service and the "
                + "notified/defaulted service path");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = "true";
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String service = "someService";
        String servicePath = "/";
        
        try {
            String builtPkgName = sink.buildPkgName(service, servicePath);
            String expectedPkgName = "somex0053ervicex002f";
        
            try {
                assertEquals(expectedPkgName, builtPkgName);
                System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                        + "-  OK  - '" + expectedPkgName + "' is equals to the encoding of "
                        + "<service>xffff<servicePath>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                        + "- FAIL - '" + expectedPkgName + "' is not equals to the encoding of "
                        + "<service>xffff<servicePath>");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildPkgNameRootServicePathEncoding
    
    /**
     * [NGSICKANSink.buildResName] -------- When no encoding, the CKAN resource name is the encoding of the
     * concatenation of the notified \<entity_id\> and \<entity_type\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildResourceNameNoEncoding() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                + "-------- When no encoding, the CKAN resource name is the encoding of the concatenation of the "
                + "notified <entityId> and <entityType>");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = "false";
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String entity = "someId=someType";
        
        try {
            String builtResName = sink.buildResName(entity);
            String expecetedResName = "someId_someType";
        
            try {
                assertEquals(expecetedResName, builtResName);
                System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                        + "-  OK  - '" + builtResName + "' is equals to the encoding of <entityId> and <entityType>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                        + "- FAIL - '" + builtResName + "' is not equals to the encoding of <entityId> and "
                        + "<entityType>");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildResourceNameNoEncoding
    
    /**
     * [NGSICKANSink.buildResName] -------- When encoding, the CKAN resource name is the encoding of the concatenation
     * of the notified \<entity_id\> and \<entity_type\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildResourceNameEncoding() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                + "-------- When encoding, the CKAN resource name is the encoding of the concatenation of the "
                + "notified <entityId> and <entityType>");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = "true";
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String entity = "someId=someType";
        
        try {
            String builtResName = sink.buildResName(entity);
            String expecetedResName = "somex0049dxffffsomex0054ype";
        
            try {
                assertEquals(expecetedResName, builtResName);
                System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                        + "-  OK  - '" + builtResName + "' is equals to the encoding of <entityId> and <entityType>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                        + "- FAIL - '" + builtResName + "' is not equals to the encoding of <entityId> and "
                        + "<entityType>");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildResourceNameEncoding
    
    /**
     * [NGSICKANSink.buildOrgName] -------- An organization name length greater than 100 characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildOrganizationNameLength() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                + "-------- An organization name length greater than 100 characters is detected");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = null; // defalt
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String service = "veryLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "ooooogService";
        
        try {
            sink.buildOrgName(service);
            System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                    + "- FAIL - An organization name length greater than 100 characters has not been detected");
            assertTrue(false);
        } catch (CygnusBadConfiguration e) {
            assertTrue(true);
            System.out.println(getTestTraceHead("[NGSICKANSink.buildOrgName]")
                    + "-  OK  - An organization name length greater than 100 characters has been detected");
        } // try catch
    } // testBuildOrganizationNameLength
    
    /**
     * [NGSICKANSink.buildPkgName] -------- A package name length greater than 100 characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildPackageNameLength() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                + "-------- A resource name length greater than 100 characters is detected");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = null; // defalt
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String service = "veryLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "ooooogService";
        String servicePath = "veryLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "ooooooooogServicePath";
        
        try {
            sink.buildPkgName(service, servicePath);
            System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                    + "- FAIL - A package name length greater than 100 characters has not been detected");
            assertTrue(false);
        } catch (CygnusBadConfiguration e) {
            assertTrue(true);
            System.out.println(getTestTraceHead("[NGSICKANSink.buildPkgName]")
                    + "-  OK  - A package name length greater than 100 characters has been detected");
        } // try catch
    } // testBuildPackageNameLength
    
    /**
     * [NGSICKANSink.buildResName] -------- A resource name length greater than 100 characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildResourceNameLength() throws Exception {
        System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                + "-------- A resource name length greater than 100 characters is detected");
        String attrPersistence = null; // default
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = null; // defalt
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();
        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username));
        String entity = "veryLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "ooooogEntity";
        
        try {
            sink.buildResName(entity);
            System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                    + "- FAIL - A resource name length greater than 100 characters has not been detected");
            assertTrue(false);
        } catch (CygnusBadConfiguration e) {
            assertTrue(true);
            System.out.println(getTestTraceHead("[NGSICKANSink.buildResName]")
                    + "-  OK  - A resource name length greater than 100 characters has been detected");
        } // try catch
    } // testBuildResourceNameLength


    @Test
    public void testPersistBatch() throws Exception {
      testPersistBatch("row", false);
      testPersistBatch("row", true);
      testPersistBatch("column", false);
      testPersistBatch("column", true);
    }

    private void testPersistBatch(final String attrPersistence, final boolean expandJson) throws Exception {
        String th = getTestTraceHead("[NGSICKANSink.persistBatch(attrPersistence=" + attrPersistence + " expandJson=" + expandJson + ")]");
        System.out.println(th + "-------- A batch must be peristed correctly");
        String batchSize = null; // default
        String batchTime = null; // default
        String batchTTL = null; // default
        String dataModel = null; // default
        String enableEncoding = null; // defalt
        String enableGrouping = null; // default
        String enableLowercase = null; // default
        String host = null; // default
        String password = null; // default
        String port = null; // default
        String username = null; // default
        NGSICKANSink sink = new NGSICKANSink();

        sink.configure(createContext(attrPersistence, batchSize, batchTime, batchTTL, dataModel, enableEncoding,
                enableGrouping, enableLowercase, host, password, port, username, expandJson));
       
        CKANBackend backend = new CKANBackend() {
          public void persist(String orgName, String pkgName, String resName, String records, boolean createEnabled)
          throws Exception {
            final String expectedRecords = 
              "row".equals(attrPersistence) ?
                expandJson ? 
                  "[{" +
                    "\"recvTimeTs\":\"12\"," + 
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," + 
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"attrName\":\"name1\"," + 
                    "\"attrType\":\"type1\"," +
                    "\"attrValue\":\"value1\"" + 
                  "},{" +
                    "\"recvTimeTs\":\"12\"," + 
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," + 
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"attrName\":\"name1_md_md1_name\"," + 
                    "\"attrType\":\"md1_type\"," +
                    "\"attrValue\":\"md1_value\"" + 
                  "},{" +
                    "\"recvTimeTs\":\"12\"," + 
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," + 
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"attrName\":\"name1_md_md2_name\"," + 
                    "\"attrType\":\"md2_type\"," +
                    "\"attrValue\":\"md2_value\"" + 
                  "},{" +
                    "\"recvTimeTs\":\"12\"," + 
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," + 
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"attrName\":\"name2_p1\"," + 
                    "\"attrType\":\"String\"," +
                    "\"attrValue\":\"v1\"" + 
                  "},{" +
                    "\"recvTimeTs\":\"12\"," + 
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," + 
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"attrName\":\"name2_p2\"," + 
                    "\"attrType\":\"Boolean\"," +
                    "\"attrValue\":true" + 
                  "},{" +
                    "\"recvTimeTs\":\"12\"," + 
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," + 
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"attrName\":\"name2_md_md1_name\"," + 
                    "\"attrType\":\"md1_type\"," +
                    "\"attrValue\":\"md1_value\"" + 
                  "},{" +
                    "\"recvTimeTs\":\"12\"," + 
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," + 
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"attrName\":\"name2_md_md2_name\"," + 
                    "\"attrType\":\"md2_type\"," +
                    "\"attrValue\":\"md2_value\"" + 
                  "}]"
                :
                  "[{" +
                    "\"recvTimeTs\":\"12\"," + 
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," + 
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"attrName\":\"name1\"," + 
                    "\"attrType\":\"type1\"," +
                    "\"attrValue\":\"value1\"," + 
                    "\"attrMd\":[" +
                      "{\"name\":\"md1_name\",\"type\":\"md1_type\",\"value\":\"md1_value\"}," +
                      "{\"name\":\"md2_name\",\"type\":\"md2_type\",\"value\":\"md2_value\"}]" +
                  "},{" +
                    "\"recvTimeTs\":\"12\"," + 
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," + 
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"attrName\":\"name2\"," + 
                    "\"attrType\":\"type2\"," +
                    "\"attrValue\":{\"p1\":\"v1\",\"p2\":true}," + 
                    "\"attrMd\":[" +
                      "{\"name\":\"md1_name\",\"type\":\"md1_type\",\"value\":\"md1_value\"}," +
                      "{\"name\":\"md2_name\",\"type\":\"md2_type\",\"value\":\"md2_value\"}]" + 
                  "}]"
               :
                expandJson ? 
                  "[{" +
                    "\"recvTimeTs\":\"12\"," +
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," +
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"name1\":\"value1\"," +
                    "\"name1_md\":[" +
                      "{\"name\":\"md1_name\",\"type\":\"md1_type\",\"value\":\"md1_value\"}," +
                      "{\"name\":\"md2_name\",\"type\":\"md2_type\",\"value\":\"md2_value\"}" + 
                    "]," +
                    "\"name2\":{\"p1\":\"v1\",\"p2\":true}," +
                    "\"name2_md\":[" +
                      "{\"name\":\"md1_name\",\"type\":\"md1_type\",\"value\":\"md1_value\"}," +
                      "{\"name\":\"md2_name\",\"type\":\"md2_type\",\"value\":\"md2_value\"}" +
                    "]" +
                  "}]"
                :
                  "[{" +
                    "\"recvTimeTs\":\"12\"," +
                    "\"recvTime\":\"1970-01-01T00:00:12.345Z\"," +
                    "\"fiwareServicePath\":\"myServicePath\"," +
                    "\"entityId\":null," + 
                    "\"entityType\":null," +
                    "\"name1\":\"value1\"," +
                    "\"name1_md\":[" +
                      "{\"name\":\"md1_name\",\"type\":\"md1_type\",\"value\":\"md1_value\"}," +
                      "{\"name\":\"md2_name\",\"type\":\"md2_type\",\"value\":\"md2_value\"}" + 
                    "]," +
                    "\"name2\":{\"p1\":\"v1\",\"p2\":true}," +
                    "\"name2_md\":[" +
                      "{\"name\":\"md1_name\",\"type\":\"md1_type\",\"value\":\"md1_value\"}," +
                      "{\"name\":\"md2_name\",\"type\":\"md2_type\",\"value\":\"md2_value\"}" +
                    "]" +
                  "}]";
            assertEquals(expectedRecords, records);
          }
        };

        sink.setPersistenceBackend(backend);

        final NotifyContextRequest request = new NotifyContextRequest();
        NotifyContextRequest.ContextElement contextElement = request.new ContextElement();
        
        final NotifyContextRequest.ContextMetadata md1 = request.new ContextMetadata();
        md1.setType("md1_type");
        md1.setName("md1_name");
        md1.setContextMetadata(new JsonPrimitive("md1_value"));

        final NotifyContextRequest.ContextMetadata md2 = request.new ContextMetadata();
        md2.setType("md2_type");
        md2.setName("md2_name");
        md2.setContextMetadata(new JsonPrimitive("md2_value"));

        final ArrayList<NotifyContextRequest.ContextMetadata> mds = new ArrayList<NotifyContextRequest.ContextMetadata>() {{
          add(md1);
          add(md2);
        }};

        final NotifyContextRequest.ContextAttribute stringAttr = request.new ContextAttribute();
        stringAttr.setType("type1");
        stringAttr.setName("name1");
        stringAttr.setContextValue(new JsonPrimitive("value1"));
        stringAttr.setContextMetadata(mds);

        final JsonObject jsonValue = new JsonObject();
        jsonValue.addProperty("p1", "v1");
        jsonValue.addProperty("p2", Boolean.TRUE);

        final NotifyContextRequest.ContextAttribute jsonAttr = request.new ContextAttribute();
        jsonAttr.setType("type2");
        jsonAttr.setName("name2");
        jsonAttr.setContextValue(jsonValue);
        jsonAttr.setContextMetadata(mds);

        ArrayList<NotifyContextRequest.ContextAttribute> attrs = new ArrayList<NotifyContextRequest.ContextAttribute>() {{
          add(stringAttr);
          add(jsonAttr);
        }};

        contextElement.setAttributes(attrs);

        NGSIBatch batch = createBatch(12345, "myService", "myServicePath", "myDestination", contextElement); 
        
        try {
            sink.persistBatch(batch);
            System.out.println(th + "- OK  - Peristence succeeded");
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
            System.out.println(th + "-  OK  - Failed to persist");
        } // try catch
    } // testPersistBatch

    private NGSIBatch createBatch(long recvTimeTs, String service, String servicePath, String destination,
            NotifyContextRequest.ContextElement contextElement) {
        NGSIEvent groupedEvent = new NGSIEvent(recvTimeTs, service, servicePath, destination, null,
            contextElement);
        NGSIBatch batch = new NGSIBatch();
        batch.addEvent(destination, groupedEvent);
        return batch;
    } // createBatch
    
    private Context createContext(String attrPersistence, String batchSize, String batchTime, String batchTTL,
            String dataModel, String enableEncoding, String enableGrouping, String enableLowercase, String host,
            String password, String port, String username) {
      return createContext(attrPersistence, batchSize, batchTime, batchTTL,
        dataModel, enableEncoding, enableGrouping, enableLowercase, host,
        password, port, username, false);
    } // createContext

    private Context createContext(String attrPersistence, String batchSize, String batchTime, String batchTTL,
            String dataModel, String enableEncoding, String enableGrouping, String enableLowercase, String host,
            String password, String port, String username, boolean expandJson) {
        Context context = new Context();
        context.put("attr_persistence", attrPersistence);
        context.put("batch_size", batchSize);
        context.put("batch_time", batchTime);
        context.put("batch_ttl", batchTTL);
        context.put("data_model", dataModel);
        context.put("enable_encoding", enableEncoding);
        context.put("enable_grouping", enableGrouping);
        context.put("enable_lowercase", enableLowercase);
        context.put("mysql_host", host);
        context.put("mysql_password", password);
        context.put("mysql_port", port);
        context.put("mysql_username", username);
        context.put("expand_json", String.valueOf(expandJson));
        return context;
    } // createContext
    
} // NGSICKANSinkTest
