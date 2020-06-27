---
title: Nearest neighbour search with Redis
date: 2020-06-27T21:42:23.504843Z

tags: Scala, Lua, Redis
---

Redis has a very nice [**geo API**](https://redis.io/commands/#geo) but it doesn’t support nearest neighbour queries. Given a point _x_, the answer to a nearest neighbour query is the closest point to _x_.

However, you’ll be able to answer nearest neighbour queries with the following Lua script. It does an exponential search over the distance parameter for [**GEORADIUS**](https://redis.io/commands/georadius) to find the nearest neighbour of a given point.

```
local dist = 100 -- Initial distance in m
local latitude = ARGV[1]
local longitude = ARGV[2]
local maxDist = tonumber(ARGV[3]) -- Max search distance in m, to avoid infinite loop on keys with no data

if not redis.call('EXISTS', KEYS[1]) then
	return nil
end

local res = redis.call('GEORADIUS', KEYS[1], longitude, latitude, dist, 'm', 'count', '1', 'asc')
while #res == 0 and dist <= maxDist do
	dist = 2 * dist
	res = redis.call('GEORADIUS', KEYS[1], longitude, latitude, dist, 'm', 'count', '1', 'asc')
end

if #res > 0 then
	return res[1]
else
	return nil
end
```

Note that the time complexity is $O(\log M \cdot \log D)$, where $M$ is the number of elements in the geo index and $D$ is the distance to the nearest neighbour \(assuming that the number of elements within distance $D$ is constant\).

To invoke the script, you need to pass the key to your geo index and three arguments: latitude, longitude, and a maximum search distance. The latter is necessary to avoid infinite loops if a key doesn’t contain any data.

If you want to use it from the shell then save the script as `nn.lua` and load the script:

```
$ redis-cli SCRIPT LOAD "$(cat nn.lua)"
"90702b22ecf3e3118ac4c3ffbd40f94c2d8a92bc"
```

The output is the SHA1 of your script. The script is now stored on your Redis instance. You only have to load it once. Now you can invoke it by issuing:

```
$ redis-cli EVALSHA 90702b22ecf3e3118ac4c3ffbd40f94c2d8a92bc 1 <key> <latitude> <longitude> <max_dist>
```

I needed to make nearest neighbour queries from an application so I wrapped the Lua script in Scala code using [**Jedis**](https://github.com/xetorthio/jedis) for connecting to Redis. Here’s a slightly simplified version of my code.

```
class NearestNeighbourSearcher(redisClient: Jedis) {
    // Get the SHA1 of the script. Assumes that it is stored in /src/main/resources/nn.lua of your project
    private val script = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("nn.lua")).getLines.mkString("\n")
    private val scriptSha: String = stringToSHA1(script)

    // Load the script if it doesn't exist on the host
    if (!redisClient.scriptExists(scriptSha)) redisClient.scriptLoad(script)

    def getNearestNeighbour(key: String, lat: Double, long: Double, maxSearchDistance: Int): Option[String] = {
        val res = redisClient.evalsha(scriptSha, 1, key, lat.toString, long.toString, maxSearchDistance.toString)
        if (res != null) 
            Some(res.asInstanceOf[String]) 
        else 
            None
    }

    def stringToSHA1(s: String): String = {
        val md = MessageDigest.getInstance("SHA-1")
        md.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
    }
}
```

---

_This post first appeared May 19, 2019 on Medium._