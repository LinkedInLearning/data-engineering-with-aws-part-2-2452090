SELECT sellerid, username, (firstname || ' ' || lastname) as name,
city, sum(qtysold)
FROM sales, date, users
WHERE sales.sellerid = users.userid
and sales.dateid = date.dateid
and year = 2008
and city = 'San Diego'
GROUP BY sellerid, username, name, city
order by 5 desc
limit 5;