/*
 * Copyright 2018 Instituut voor Nederlandse Taal (INT).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.inl.blacklab.index;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author eduard
 */
public class TestYaml {
    
    @Test
    public void testDuplicatObjects() {
        DocIndexerFactoryConfig factoryConfig = new DocIndexerFactoryConfig() {
            @Override
            public boolean isSupported(String formatIdentifier) {
                return "nodups".equals(formatIdentifier);
            }
            
        };
        
        try {
            factoryConfig.load("nodups", new File("src/test/resources/yaml/nodups.blf.yaml"));
            Assert.fail("expected duplicates error");
        } catch (IllegalArgumentException ex) {
            Assert.assertTrue(ex.getMessage().equals("A config format with this name already exists."));
        }
    }
    
}
