USE wiki_project;

-- Return the list of existing domains for which pages were created.
INSERT INTO existing_domains (domain)
VALUES ('french');
INSERT INTO existing_domains (domain)
VALUES ('french');
INSERT INTO existing_domains (domain)
VALUES ('english');

select * from existing_domains;

-- Return all the pages which were created by the user with a specified user_id.
INSERT INTO pages_created_by_user_id (user_id, page_title, page_id)
VALUES ('12', 'test', '56');
INSERT INTO pages_created_by_user_id (user_id, page_title, page_id)
VALUES ('12', 'test2', '66');
INSERT INTO pages_created_by_user_id (user_id, page_title, page_id)
VALUES ('16', 'test5', '69');

select page_id, page_title from pages_created_by_user_id where user_id='12';

-- Return the number of articles created for a specified domain.
INSERT INTO domains_articles (domain, page_id)
VALUES ('french', '10');
INSERT INTO domains_articles (domain, page_id)
VALUES ('french', '12');
INSERT INTO domains_articles (domain, page_id)
VALUES ('french', '13');

select count(page_id) from domains_articles where domain='french';

-- Return the page with the specified page_id
INSERT INTO pages_by_id (page_id, page_title)
VALUES ('10', 'test');
INSERT INTO pages_by_id (page_id, page_title)
VALUES ('12', 'test2');

select page_title from pages_by_id where page_id='12';

-- Return the id, name, and the number of created pages of all the users
-- who created at least one page in a specified time range.
INSERT INTO user_pages_by_hour (hour, user_id, user_name, page_id)
VALUES (10, '100', 'user', '342');
INSERT INTO user_pages_by_hour (hour, user_id, user_name, page_id)
VALUES (10, '100', 'user', '564');
INSERT INTO user_pages_by_hour (hour, user_id, user_name, page_id)
VALUES (10, '102', 'user2', '5624');
INSERT INTO user_pages_by_hour (hour, user_id, user_name, page_id)
VALUES (11, '100', 'user', '6');

select user_id, user_name, count(page_id) from user_pages_by_hour where hour=10 group by user_id;


INSERT INTO category_a (datetime, domain, user_is_bot, user_name, user_id, page_title, page_id)
VALUES ('2022-09-01', 'fr', true, 'test', 1234, 'ytest', 1234);