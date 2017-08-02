/**
 * 本项目尝试通过 Mqtt 达成 RethinkDB 的 changefeeds 的订阅与推送
 * 基本思路如下：
 * - 通过 watch 会话发送请求，请求会提交随机生成的 topic，服务端订阅 rethinkdb 的 changefeeds，并将rethinkdb 后续数据发送到上述 topic
 * - 通过定时向 keep 发送之前生成的 随机topic 来保持订阅的有效性
 * - 服务端在指定时间后未收到 keep 请求则主动移除 changefeeds 订阅
 * - 客户端此时 发送 keep 请求将收到 error 信息
 */
const r = require('rethinkdbdash')();
const mqttClient = require('mqtt').connect('mqtt://localhost');
// const mqttClient = mqtt;
// console.log(mqttClient);

const { createKeeper } = require('./lib/keeper');
let keepers = {}


mqttClient.on('connect', function () {
    console.log('mqtt server connected!');
    mqttClient.subscribe('test')
    mqttClient.subscribe('watch')
    mqttClient.subscribe('keep')
})


mqttClient.on('message', function (topic, message, packet) {
    console.log('got message...', topic, message.toString())
    topicHandle[topic](message)
})

mqttClient.on('error', error => console.log('got error >>>', error))
// mqttClient.on('packetreceive', packet => console.log('packetreceive >>>',packet))



const topicHandle = {
    // 提交查询
    'watch': (payload) => {
        let { table, topic } = JSON.parse(payload.toString());
        r.table(table).changes().run().then(cursor => {
            let keeper = keepers[topic] = createKeeper(
                function () {
                    // 超时回调函数，清理后事
                    cursor.close();
                    delete keepers[topic];
                }, 10);
            keeper.start();
            cursor.each(function (err, result) {
                console.log('::::changed::::', result);
                mqttClient.publish(topic, JSON.stringify(result));
            })
        })
    },
    // 保持推送
    'keep': function (payload) {
        console.log(Object.keys(keepers));
        if (keepers[payload]) {

            keepers[payload].touch();
        } else {
            mqttClient.publish(payload, '"error"')
        }
    },
    'test': (payload) => console.log('test: ', payload.toString())
}



//  watch test 表的数据
// console.log(mqttClient);
// let watchTopic = mqttClient.options.clientId + '_watch'
// setTimeout(function () { mqttClient.publish('watch', `{"topic":"${watchTopic}","table":"test"}`); }, 2000)


// // 保持连接
// let keepCount = 3;
// let keepIntervalId = setInterval(function () {
//     mqttClient.publish('keep', watchTopic)
//     keepCount--;
//     keepCount || clearInterval(keepIntervalId);
// },5000)