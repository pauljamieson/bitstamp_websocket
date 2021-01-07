SELECT table_schema "bistamp",CREATE TABLE `ethusd` (
  `_id` int unsigned NOT NULL AUTO_INCREMENT,
  `id` int unsigned NOT NULL,
  `buy_order_id` bigint unsigned NOT NULL,
  `sell_order_id` bigint unsigned NOT NULL,
  `amount` decimal(12,8) unsigned NOT NULL,
  `price` decimal(16,8) unsigned NOT NULL,
  `type` int unsigned NOT NULL,
  `timestamp` int unsigned NOT NULL,
  PRIMARY KEY (`_id`),
  UNIQUE KEY `_id_UNIQUE` (`_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
