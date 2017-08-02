module.exports = {
    createKeeper: function createKeeper(cb, initCount) {
        let keeper = {
            initCount: initCount || 10,
            count: 0,
            intervalId: null,
            touch: function () {
                keeper.count = keeper.initCount;
            },
            start: function () {
                keeper.touch();
                keeper.intervalId = setInterval(function () {
                    console.log(keeper.count)
                    keeper.count--;
                    if (keeper.count === 0) {
                        console.log('time out!');
                        keeper.stop();
                        cb();
                    };
                }, 1000);
            },
            stop: function () {
                clearInterval(keeper.intervalId);
            }
        }
        return keeper;
    }
}