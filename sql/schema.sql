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
