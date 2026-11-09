using System;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using OmiLAXR.ReCoPa.Network;

namespace OmiLAXR.ReCoPa.Tests
{
    public class ReCoPaTypes_Tests
    {
        [Test]
        public void NewtonsoftJsonSerializer_Serialize()
        {
            // Arrange
            var serializer = new NewtonsoftJsonSerializer();

            // Assert
            Assert.AreEqual(string.Empty, serializer.Serialize(null));
            Assert.AreEqual("raw", serializer.Serialize("raw"));
            Assert.AreEqual("{\"a\":1}", serializer.Serialize(JObject.Parse("{ \"a\": 1 }")));
            Assert.AreEqual("{\"b\":true}", serializer.Serialize(new { b = true }));
        }

        [Test]
        public void NewtonsoftJsonSerializer_Deserialize()
        {
            // Arrange
            var serializer = new NewtonsoftJsonSerializer();

            // Assert
            Assert.AreEqual(3, serializer.Deserialize<int>("3"));
            Assert.AreEqual(0, serializer.Deserialize<int>(" "));
            Assert.IsNull(serializer.Deserialize<string>(""));
        }

        [Test]
        public void SocketResponse_Exposes_Raw_Text_Value_And_Token()
        {
            // Arrange
            var response = new SocketResponse("{\"isTracking\":true}", new NewtonsoftJsonSerializer());

            // Assert
            Assert.AreEqual("{\"isTracking\":true}", response.RawText);
            Assert.IsTrue(response.GetValue<TrackingMeta>().isTracking);
            Assert.AreEqual(true, (bool)response.GetToken()["isTracking"]);
        }

        [Test]
        public void SocketResponse_Empty_Payload_Is_Null_Token()
        {
            // Arrange
            var response = new SocketResponse(null, new NewtonsoftJsonSerializer());

            // Assert
            Assert.AreEqual(string.Empty, response.RawText);
            Assert.AreEqual(JTokenType.Null, response.GetToken().Type);
            Assert.Throws<ArgumentNullException>(() => new SocketResponse("{}", null));
        }

        [Test]
        public void ReconnectConfiguration_Presets_Are_Ordered_By_Aggressiveness()
        {
            // Act
            var aggressive = ReconnectConfiguration.CreateAggressiveConfig();
            var moderate = ReconnectConfiguration.CreateModerateConfig();
            var conservative = ReconnectConfiguration.CreateConservativeConfig();
            ReconnectConfiguration.Example();

            // Assert
            foreach (var o in new[] { aggressive, moderate, conservative })
            {
                Assert.IsTrue(o.Reconnection);
                Assert.AreEqual(-1, o.ReconnectionAttempts);
                Assert.IsTrue(o.EnableHeartbeat);
                Assert.Less(o.HeartbeatTimeoutMs, o.HeartbeatIntervalMs);
                Assert.LessOrEqual(o.ReconnectionDelay, o.ReconnectionDelayMax);
            }
            Assert.Less(aggressive.ReconnectionDelay, moderate.ReconnectionDelay);
            Assert.Less(moderate.ReconnectionDelay, conservative.ReconnectionDelay);
            Assert.Less(aggressive.HeartbeatIntervalMs, conservative.HeartbeatIntervalMs);
        }

        [Test]
        public void TrackingConfig_ToString_Does_Not_Leak_Secret()
        {
            // Arrange
            var config = new TrackingConfig
            {
                lrs = "https://lrs.example.org",
                auth = new TrackingConfig.ClientAuth("key", "top-secret"),
                identity = new TrackingConfig.TrackingIdentity("Learner", "learner@omilaxr.dev"),
                actions = new[] { "a", "b" },
            };

            // Act
            var text = config.ToString();

            // Assert
            StringAssert.Contains("lrs=https://lrs.example.org", text);
            StringAssert.Contains("email=learner@omilaxr.dev", text);
            StringAssert.Contains("actions=[Array: [a,b]]", text);
            StringAssert.Contains("gestures=[Array: []]", text);
            StringAssert.DoesNotContain("top-secret", text);
        }

        [Test]
        public void TrackingScenario_ToString_Lists_Arrays()
        {
            // Arrange
            var scenario = new TrackingScenario { name = "Lab", actions = new[] { "grab" } };

            // Assert
            Assert.AreEqual("[TrackingScenario name=Lab, actions=[Array: [grab]], gestures=[Array: []], gameObjects=[Array: []]]",
                scenario.ToString());
        }

        [Test]
        public void TrackingMeta_Empty_Has_Defaults()
        {
            Assert.IsFalse(TrackingMeta.Empty.isTracking);
            Assert.IsNull(TrackingMeta.Empty.heartRate);
        }
    }
}
