package ir.seefa.mapper;

import ir.seefa.model.Customer;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Saman Delfani
 * @version 1.0
 * @since 2022-07-30 23:28:17
 */
public class CustomerRowMapper implements RowMapper<Customer> {

    @Override
    public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
        Customer customer = new Customer();

        customer.setCustomerNumber(rs.getInt("customerNumber"));
        customer.setCustomerName(rs.getString("customerName"));
        customer.setContactLastName(rs.getString("contactLastName"));
        customer.setContactFirstName(rs.getString("contactFirstName"));
        customer.setPhone(rs.getString("phone"));
        customer.setAddressLine1(rs.getString("addressLine1"));
        customer.setAddressLine2(rs.getString("addressLine2"));
        customer.setCity(rs.getString("city"));
        customer.setState(rs.getString("state"));
        customer.setPostalCode(rs.getString("postalCode"));
        customer.setCountry(rs.getString("country"));
        customer.setSalesRepEmployeeNumber(rs.getInt("salesRepEmployeeNumber"));
        customer.setCreditLimit(rs.getDouble("creditLimit"));

        return customer;
    }
}
