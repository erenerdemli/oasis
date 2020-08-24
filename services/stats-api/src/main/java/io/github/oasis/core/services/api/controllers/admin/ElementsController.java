/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.oasis.core.services.api.controllers.admin;

import io.github.oasis.core.elements.ElementDef;
import io.github.oasis.core.exception.OasisException;
import io.github.oasis.core.services.ElementCRUDSupport;
import io.github.oasis.core.services.api.controllers.AbstractController;
import io.github.oasis.core.services.exceptions.OasisApiException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Set;

/**
 * @author Isuru Weerarathna
 */
@RestController
@RequestMapping(
    consumes = MediaType.APPLICATION_JSON_VALUE,
    produces = MediaType.APPLICATION_JSON_VALUE
)
public class ElementsController extends AbstractController {

    private final Map<String, ElementCRUDSupport> crudSupportMap;

    public ElementsController(Map<String, ElementCRUDSupport> crudSupportMap) {
        this.crudSupportMap = crudSupportMap;
    }

    @GetMapping(path = "/admin/games/{gameId}/elements")
    public Set<String> getGameElementTypes(@PathVariable("gameId") Integer gameId) {
        return crudSupportMap.keySet();
    }

    @GetMapping(path = "/admin/games/{gameId}/elements/{elementType}/{elementId}")
    public ElementDef read(@PathVariable("gameId") Integer gameId,
                           @PathVariable("elementType") String elementType,
                           @PathVariable("elementId") String elementId) throws OasisException {
        ElementCRUDSupport elementCRUDSupport = crudSupportMap.get(elementType);
        if (elementCRUDSupport == null) {
            throw new OasisApiException("E40400", HttpStatus.NOT_FOUND.value(), "Unknown element type! [" + elementType + "]");
        }
        return elementCRUDSupport.readElementDefinition(gameId, elementId);
    }

    @PostMapping(path = "/admin/games/{gameId}/elements/{elementType}")
    public ElementDef add(@PathVariable("gameId") Integer gameId,
                           @PathVariable("elementType") String elementType,
                           @RequestBody ElementDef elementDef) throws OasisException {
        ElementCRUDSupport elementCRUDSupport = crudSupportMap.get(elementType);
        if (elementCRUDSupport == null) {
            throw new OasisApiException("E40400", HttpStatus.NOT_FOUND.value(), "Unknown element type! [" + elementType + "]");
        }
        return elementCRUDSupport.addElementDefinition(gameId, elementDef.getId(), elementDef);
    }

    @PutMapping(path = "/admin/games/{gameId}/elements/{elementType}/{elementId}")
    public ElementDef update(@PathVariable("gameId") Integer gameId,
                             @PathVariable("elementType") String elementType,
                             @PathVariable("elementId") String elementId,
                             @RequestBody ElementDef elementDef) throws OasisException {
        ElementCRUDSupport elementCRUDSupport = crudSupportMap.get(elementType);
        if (elementCRUDSupport == null) {
            throw new OasisApiException("E40400", HttpStatus.NOT_FOUND.value(), "Unknown element type! [" + elementType + "]");
        }
        return elementCRUDSupport.updateElementDefinition(gameId, elementDef.getId(), elementDef);
    }

    @DeleteMapping(path = "/admin/games/{gameId}/elements/{elementType}/{elementId}")
    public ElementDef delete(@PathVariable("gameId") Integer gameId,
                             @PathVariable("elementType") String elementType,
                             @PathVariable("elementId") String elementId) throws OasisException {
        ElementCRUDSupport elementCRUDSupport = crudSupportMap.get(elementType);
        if (elementCRUDSupport == null) {
            throw new OasisApiException("E40400", HttpStatus.NOT_FOUND.value(), "Unknown element type! [" + elementType + "]");
        }
        return elementCRUDSupport.deleteElementDefinition(gameId, elementId);
    }
}