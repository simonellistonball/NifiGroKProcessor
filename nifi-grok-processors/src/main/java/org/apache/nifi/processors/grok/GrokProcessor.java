/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.grok;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

@Tags({"Grok Processor"})
@CapabilityDescription("Use Grok expression ,a la logstash, to parse data.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GrokProcessor extends AbstractProcessor {

    public static final PropertyDescriptor GROK_EXPRESSION = new PropertyDescriptor
            .Builder().name("Grok Expression")
            .description("Grok expression")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

  public static final PropertyDescriptor GROK_PATTERN_FILE = new PropertyDescriptor
          .Builder().name("Grok Pattern file")
          .description("Grok Pattern file definition")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor
            .Builder().name("Character Set")
            .description("The Character Set in which the file is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor
            .Builder().name("Maximum Buffer Size")
            .description("Specifies the maximum amount of data to buffer (per file) in order to apply the regular expressions. Files larger than the specified maximum will not be fully evaluated.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .addValidator(StandardValidators.createDataSizeBoundsValidator(0, Integer.MAX_VALUE))
            .defaultValue("1 MB")
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship when the Regular Expression is successfully evaluated and the FlowFile is modified as a result")
            .build();

  public static final Relationship REL_NO_MATCH = new Relationship.Builder()
          .name("unmatched")
          .description("FlowFiles are routed to this relationship when no provided Regular Expression matches the content of the FlowFile")
          .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private Grok grok;
    private byte[] buffer;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(GROK_EXPRESSION);
        descriptors.add(GROK_PATTERN_FILE);
        descriptors.add(CHARACTER_SET);
        descriptors.add(MAX_BUFFER_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws GrokException {

      final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
      buffer = new byte[maxBufferSize];

      grok = Grok.create(context.getProperty(GROK_PATTERN_FILE).getValue());
      grok.compile(context.getProperty(GROK_EXPRESSION).getValue());

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

      final ProcessorLog logger = getLogger();
      final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
      final Map<String, String> grokResults = new HashMap<>();
      final byte[] byteBuffer = buffer;
      session.read(flowFile, new InputStreamCallback() {
          @Override
          public void process(InputStream in) throws IOException {
            StreamUtils.fillBuffer(in, byteBuffer, false);
          }
      });
      final long len = Math.min(byteBuffer.length, flowFile.getSize());
      final String contentString = new String(byteBuffer, 0, (int) len, charset);


      logger.info("Content String: "+contentString);
      final Match gm = grok.match(contentString);
      gm.captures();
      logger.info("Matched {} Regular Expressions "+gm.toJson());
      for(Map.Entry<String,Object> entry: gm.toMap().entrySet()){
        if(null != entry.getValue() && context.getProperty(GROK_EXPRESSION).getValue().contains(entry.getKey())) {
          grokResults.put(entry.getKey(), entry.getValue().toString());
        }
      }
      if (!grokResults.isEmpty()) {
        //flowFile = session.removeAllAttributes(flowFile, Pattern.compile(".*"));
        flowFile = session.putAllAttributes(flowFile, grokResults);

        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_MATCH);
        logger.info("Matched {} Regular Expressions and added attributes to FlowFile {}", new Object[]{grokResults.size(), flowFile});
      } else {
        session.transfer(flowFile, REL_NO_MATCH);
        logger.info("Did not match any Regular Expressions for FlowFile {}", new Object[]{flowFile});
      }
    }




}
