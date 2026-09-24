package com.orderflow.inventory.controller;

import com.orderflow.inventory.service.InventoryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(InventoryController.class)
class InventoryControllerWebMvcTest {

    @Autowired
    private MockMvc mockMvc;
    @MockBean
    private InventoryService inventoryService;

    @Test
    void getStockReturnsAvailableQuantity() throws Exception {
        when(inventoryService.getAvailableStock(3L)).thenReturn(70);

        mockMvc.perform(get("/api/inventory/3/stock"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.productId").value(3))
                .andExpect(jsonPath("$.availableQuantity").value(70));
    }

    @Test
    void getStockWithNonNumericIdIsBadRequest() throws Exception {
        mockMvc.perform(get("/api/inventory/abc/stock"))
                .andExpect(status().isBadRequest());
    }
}