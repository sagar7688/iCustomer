CREATE TABLE user_interaction_data (
	interaction_id varchar NULL,
	user_id int8 NULL,
	product_id varchar NULL,
	"action" text NULL,
	"timestamp" timestamp NULL,
	interaction_count int8 NULL
);