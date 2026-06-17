CREATE DATABASE IF NOT EXISTS movie_analysis
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE movie_analysis;

CREATE TABLE IF NOT EXISTS streaming_results (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  run_id VARCHAR(64) NOT NULL,
  batch_time DATETIME NOT NULL,
  movie_id INT NOT NULL,
  title VARCHAR(255) NOT NULL,
  avg_rating DECIMAL(4, 2) NOT NULL,
  rating_count INT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_run_id (run_id),
  INDEX idx_batch_time (batch_time),
  INDEX idx_movie_id (movie_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
 
 -- ============================================================
 -- 任务二：Spark SQL 性别标签偏好 — users & ratings 表
 -- ============================================================
 
 CREATE TABLE IF NOT EXISTS users (
   userId INT PRIMARY KEY,
   gender CHAR(1) NOT NULL COMMENT 'M=男, F=女',
   age INT,
   occupation INT,
   zipCode VARCHAR(10)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
 
 CREATE TABLE IF NOT EXISTS ratings (
   userId INT NOT NULL,
   movieId INT NOT NULL,
   rating DECIMAL(2,1) NOT NULL,
   timestamp INT NOT NULL,
   PRIMARY KEY (userId, movieId, timestamp),
   INDEX idx_userId (userId),
   INDEX idx_movieId (movieId)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
