#!python2.7
import psycopg2


def get_query_results(query):
    db = psycopg2.connect(database="news")
    c = db.cursor()
    c.execute(query)
    result = c.fetchall()
    db.close()
    return result


def most_popular_articles():

    query1 = '''select articles.title,
            count(*) as number
            from public.articles
            inner join public.log
            on log.path = '/article/' || articles.slug
            group by articles.title
            order by number desc
            limit 3;'''
    result = get_query_results(query1)
    print "\nWhat are the top three articles of all time?\n"
    for article, views in result:
        print "    {}  --  {} views".format(article, views)


def most_popular_authors():
    query2 = '''select authors.name, count(*) as number
            from public.authors
            inner join public.articles
            on authors.id = articles.author
            join public.log
            on log.path = '/article/' || articles.slug
            group by authors.name
            order by number desc
                    limit 3;'''
    result = get_query_results(query2)
    print "\nWhat are the top three authors of all time?\n"
    for name, views in result:
        print "    {}  --  {} views".format(name, views)


def my_percentage():
    query3 = '''with errors as
            (select date(time), count(status) as err
                        from log where status ='404 NOT FOUND'
                        group by date(time)
                        ),total as
                        (select  date(time), count(*) as global
                        from log
                        group by date(time)
                        )
            select to_char(errors.date, 'FMMonth FMDD, YYYY') as
            date,
            (err /global::float) as percentage
            from errors inner join total
            on errors.date = total.date
            where (err /global::float) > 0.01;'''
    result = get_query_results(query3)
    print "\nOn which days did more than 1% of requests lead to errors?\n"
    for row in result:
        print row[0], " -- ", round(row[1] * 100, 2), "%"


def main():
    most_popular_articles()
    most_popular_authors()
    my_percentage()


if __name__ == '__main__':
    main()
