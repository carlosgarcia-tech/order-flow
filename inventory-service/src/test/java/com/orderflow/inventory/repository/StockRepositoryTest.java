package com.orderflow.inventory.repository;

import com.orderflow.inventory.domain.Product;
import com.orderflow.inventory.domain.Stock;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
class StockRepositoryTest {

    @Autowired
    private ProductRepository productRepository;
    @Autowired
    private StockRepository stockRepository;

    @Test
    void findByProductIdForUpdateReturnsStock() {
        Product product = productRepository.save(
                Product.builder().name("Cable HDMI").sku("SKU-HDMI").build());
        stockRepository.save(Stock.builder()
                .product(product)
                .quantityAvailable(10)
                .quantityReserved(0)
                .build());

        assertThat(stockRepository.findByProductId(product.getId())).isPresent();

        Stock locked = stockRepository.findByProductIdForUpdate(product.getId()).orElseThrow();
        assertThat(locked.getQuantityAvailable()).isEqualTo(10);
        assertThat(locked.getQuantityReserved()).isZero();
        assertThat(locked.getProduct().getName()).isEqualTo("Cable HDMI");
    }
}