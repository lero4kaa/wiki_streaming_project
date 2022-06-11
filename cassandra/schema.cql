CREATE KEYSPACE wiki_project WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE wiki_project;

-- CATEGORY B
-- Return the list of existing domains for which pages were created.
CREATE TABLE existing_domains
(
    domain text,
    PRIMARY KEY (domain)
);


-- Return all the pages which were created by the user with a specified user_id.
CREATE TABLE pages_created_by_user_id
(
    user_id    text,
    page_title text,
    page_id    text,
    PRIMARY KEY ((user_id), page_id)
);


-- Return the number of articles created for a specified domain.
CREATE TABLE domains_articles
(
    domain  text,
    page_id text,
    PRIMARY KEY ((domain), page_id)
);


-- Return the page with the specified page_id
CREATE TABLE pages_by_id
(
    page_id    text,
    page_title text,
    PRIMARY KEY (page_id)
);


-- Return the id, name, and the number of created pages of all the users
-- who created at least one page in a specified time range.
CREATE TABLE user_pages_by_hour
(
    hour      int,
    user_id   text,
    user_name text,
    page_id   text,
    PRIMARY KEY ((hour), user_id, page_id)
);


-- Data for category a
CREATE TABLE category_a
(
    datetime timestamp,
    domain text,
    user_is_bot boolean,
    user_name text,
    user_id int,
    page_title text,
    page_id int,
    message_id text,
    PRIMARY KEY ((datetime), message_id)
);

-- category A results
CREATE TABLE first_request
(
    time_start text,
    statistics text,
    PRIMARY KEY ((time_start))
);

CREATE TABLE second_request
(
    time_start text,
    statistics text,
    PRIMARY KEY ((time_start))
);

CREATE TABLE third_request
(
    time_start text,
    statistics text,
    PRIMARY KEY ((time_start))
);