from datetime import datetime
from pyspark import SparkContext, SparkConf
import re
ENABLE_LOGS = False

def parse_data(x):
    """Parse NASA data from text.
    """
    try:
        regex_result = \
            re.compile(
                '(.*) - - \[(.*):(.*):(.*):(.*)\] "(.*)" ([0-9]*) ([0-9]*|-)') \
                .match(x)
        host = regex_result.group(1)
        ord_day = \
            datetime.strptime(regex_result.group(2), '%d/%b/%Y').toordinal()
        request = regex_result.group(6)
        reply_code = int(regex_result.group(7))

        try:
            reply_bytes = int(regex_result.group(8))
        except ValueError as e:
            # Byte size field has '-'.
            reply_bytes = 0
        return host, ord_day, request, reply_code, reply_bytes
    except Exception as e:
        if ENABLE_LOGS:
            print e
            print "[ERROR] Error while parsing data: %s" % x
        return "", -1, "", -1, -1

def get_url(x):
    """Get url from data.
    """
    try:
        regex_result = \
            re.compile(
                '(GET|HEAD|POST) (.*)') \
                .match(x[2])
        url = x[0] + regex_result.group(2).split()[0]
        return url
    except Exception as e:
        if ENABLE_LOGS:
            print e
            print "[ERROR] Error while parsing url: %s" % x[2]
        return ""

def date_to_str(x):
    """Convert date to string.
    """
    try:
        return datetime.fromordinal(x).strftime('%d/%b/%Y')
    except Exception as e:
        if ENABLE_LOGS:
            print e
            print "[ERROR] Error during date conversion: ", x
        return "01/01/1900"

if __name__ == '__main__':
    # Initialize spark context.
    conf = SparkConf().setAll(\
        [('spark.executor.memory', '4g'), \
         ('spark.executor.cores', '4'), \
         ('spark.cores.max', '4'), \
         ('spark.driver.memory','4g')])
    sc=SparkContext(conf=conf)

    # Read raw text to RDD.
    lines=sc.textFile('resources')
    rdd=lines.map(parse_data)

    # Filter invalid data.
    rdd = rdd.filter(lambda x: x[1] > -1)
    rdd.cache

    # Compute number of unique hosts.
    number_hosts = rdd.keys().distinct().count()

    # Get total of 404 errors.
    r_404_errors = rdd.filter(lambda x: x[3] == 404)
    r_404_errors.cache
    total_404_errors = r_404_errors.count()

    # Compute Top 5 URLs with 404 errors.
    url_data = r_404_errors.map(lambda s: (get_url(s), 1))
    url_counts = url_data.reduceByKey(lambda a, b: a + b)
    url_counts_sorted = url_counts.sortBy(keyfunc=lambda x: x[1], \
                                          ascending=False)
    top_5_urls_404_data = url_counts_sorted.map(lambda x: x[0]).take(5)
    top_5_urls_404 = ''.join("\n     %s " % url for url in top_5_urls_404_data)

    # Compute total of 404 errors by day.
    r_404_errors_by_day = r_404_errors.map(lambda s: (s[1], 1))
    r_404_errors_by_day_counts = \
        r_404_errors_by_day.reduceByKey(lambda a, b: a + b).sortByKey()
    r_404_errors_by_day_counts_data = r_404_errors_by_day_counts.collect()
    r_404_errors_by_day_counts_values = \
        r_404_errors_by_day_counts.map(lambda s: s[1])
    r_404_errors_by_day_counts_values.cache
    mean_404_errors_by_day = r_404_errors_by_day_counts_values.mean()
    std_404_errors_by_day = r_404_errors_by_day_counts_values.stdev()
    stats_404_errors_by_day = \
        "%.2f +- %.2f" % (mean_404_errors_by_day, std_404_errors_by_day)

    i = 1
    total_404_errors_by_day = "\n"
    for date_count in r_404_errors_by_day_counts_data:
        if i % 5 == 1:
            total_404_errors_by_day += "          "

        total_404_errors_by_day += \
            "%s: %d" % (date_to_str(date_count[0]), date_count[1])

        if i % 5 == 0:
            total_404_errors_by_day += "\n"
        else:
            total_404_errors_by_day += "     "
        i += 1

    # Compute total of bytes returned.
    unity = "GB"
    if unity == "GB":
        unity_rate = 1073741824.0
    else:
        unity_rate = 1073741824.0 * 1024
    total_bytes_data = rdd.map(lambda s: s[4] / unity_rate)
    total_bytes = total_bytes_data.sum()

    # Print results.
    print "============================================"
    print "1. Number of unique hosts : %d" % number_hosts
    print "2. Total of 404 errors: %d" % total_404_errors
    print "3. Top 5 URLs with 404 errors: %s\n" % top_5_urls_404
    print "4. 404 errors by day:\n     Mean and Std.: %s\n" \
        % stats_404_errors_by_day
    print "     Total of 404 errors by day:%s\n" \
        % total_404_errors_by_day
    print "5. Total of bytes returned: %.2f %s" % (total_bytes, unity)
