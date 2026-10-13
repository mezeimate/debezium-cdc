USE bank;

CREATE TABLE ACCOUNT (
    id BIGINT NOT NULL PRIMARY KEY,
    owner_name VARCHAR(100) NOT NULL,
    balance DECIMAL(12,2) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'HUF',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;

CREATE TABLE PAYMENT (
    id BIGINT NOT NULL PRIMARY KEY,
    account_id BIGINT NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'HUF',
    status VARCHAR(20) NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    CONSTRAINT fk_payment_account FOREIGN KEY (account_id) REFERENCES ACCOUNT(id)
) ENGINE=InnoDB;

INSERT INTO ACCOUNT (id, owner_name, balance, currency) VALUES
    (1001, 'Kiss Anna', 150000.00, 'HUF'),
    (1002, 'Nagy Peter', 250000.00, 'HUF');

INSERT INTO PAYMENT (id, account_id, amount, currency, status) VALUES
    (1, 1001, 12500.00, 'HUF', 'COMPLETED'),
    (2, 1002, 3400.00, 'HUF', 'PENDING');

-- Fixed local lab credentials; not intended for a public/production database.
CREATE USER 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'debezium_lab_password';
GRANT SELECT, LOCK TABLES ON bank.* TO 'debezium'@'%';
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
