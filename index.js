/**
 * 本项目尝试通过 Mqtt 达成 RethinkDB 的 changefeeds 的订阅与推送
 * 基本思路如下：
 * - 通过 watch 会话发送请求，请求会提交随机生成的 topic，服务端订阅 rethinkdb 的 changefeeds，并将rethinkdb 后续数据发送到上述 topic
 * - 通过定时向 keep 发送之前生成的 随机topic 来保持订阅的有效性
 * - 服务端在指定时间后未收到 keep 请求则主动移除 changefeeds 订阅
 * - 客户端此时 发送 keep 请求将收到 error 信息
 */
const r = require('rethinkdbdash')();
const mqtt = require('mqtt');
const mqttClient = mqtt.connect('mqtt://localhost');

const { createKeeper } = require('./lib/keeper');
let keepers = {}


mqttClient.on('connect', function () {
    mqttClient.subscribe('test')
    mqttClient.subscribe('watch')
    mqttClient.subscribe('keep')
})

mqttClient.on('message', function (topic, message) {
    console.log('got message...', topic, message.toString())
    topicHandle[topic](message)
})


const topicHandle = {
    // 提交查询
    'watch': (message) => {
        let { table, topic } = JSON.parse(message.toString());
        r.table(table).changes().run().then(cursor => {
            let tm = keepers[topic] = createKeeper(function () {
                cursor.close();
                delete keepers[topic];
            }, 10);
            tm.start();
            cursor.each(function (err, result) {
                console.log('::::changed::::', result);
                mqttClient.publish(topic, JSON.stringify(result));
            })
        })
    },
    // 保持推送
    'keep': function (topic) {
        console.log( Object.keys(keepers));
        if (keepers[topic]) {
            
          keepers[topic].touch();
        } else {
            mqttClient.publish(topic,'error')
        }
    },
    'test': (message) => console.log('test: ', message.toString())
}



//  watch test 表的数据
setTimeout(function () { mqttClient.publish('watch', '{"topic":"aEdljf12","table":"test"}'); }, 2000)


// 保持连接
let keepCount = 3;
let keepIntervalId = setInterval(function () {
    mqttClient.publish('keep', 'aEdljf12');
    keepCount--;
    keepCount || clearInterval(keepIntervalId);
},5000)