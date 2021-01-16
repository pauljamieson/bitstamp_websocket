CREATE TABLE `watchers` (
  `_id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(45) NOT NULL,
  `channel` varchar(45) NOT NULL,
  `currency_pair` varchar(45) NOT NULL,
  PRIMARY KEY (`_id`),
  UNIQUE KEY `_id_UNIQUE` (`_id`),
  UNIQUE KEY `name_UNIQUE` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci