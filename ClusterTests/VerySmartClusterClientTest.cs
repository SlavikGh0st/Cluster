using System;
using System.Diagnostics;
using System.Linq;
using ClusterClient.Clients;
using FluentAssertions;
using NUnit.Framework;

namespace ClusterTests
{
	public class VerySmartClusterClientTest : SmartClusterTest
	{
		protected override SmartClusterClientBase CreateClient(string[] replicaAddresses)
			=> new VerySmartClusterClient(replicaAddresses);

		[Test]
		public override void Client_should_return_success_when_timeout_is_close()
		{
			for(int i = 0; i < 3; i++)
				CreateServer(Timeout / 3);

			LastTurnResultProcessRequests(Timeout + 100);
		}

		[Test]
		public void ShouldReturnSuccessWhenLastReplicaIsGoodAndOthersAreSlow()
		{
			for (int i = 0; i < 3; i++)
				CreateServer(Slow);
			CreateServer(Fast);

			LastTurnResultProcessRequests(Timeout).Last().Should().BeCloseTo(TimeSpan.FromMilliseconds(3 * Timeout / 4 + Fast), Epsilon);
		}

		[Test]
		public void ShouldReturnSuccessWhenLastReplicaIsGoodAndOthersAreBad()
		{
			for (int i = 0; i < 3; i++)
				CreateServer(1, status: 500);
			CreateServer(Fast);

			LastTurnResultProcessRequests(Timeout).Last().Should().BeCloseTo(TimeSpan.FromMilliseconds(Fast), Epsilon);
		}

		[Test]
		public void ShouldThrowAfterTimeout()
		{
			for (var i = 0; i < 10; i++)
				CreateServer(Slow);

			var sw = Stopwatch.StartNew();
			Assert.Throws<TimeoutException>(() => LastTurnResultProcessRequests(Timeout));
			sw.Elapsed.Should().BeCloseTo(TimeSpan.FromMilliseconds(Timeout), Epsilon);
		}

		[Test]
		public void ShouldNotForgetPreviousAttemptWhenStartNew()
		{
			CreateServer(4500);
			CreateServer(3000);
			CreateServer(10000);

			foreach(var time in LastTurnResultProcessRequests(6000))
				time.Should().BeCloseTo(TimeSpan.FromMilliseconds(4500), Epsilon);
		}

		[Test]
		public void ShouldNotSpendTimeOnBad()
		{
			CreateServer(1, status: 500);
			CreateServer(1, status: 500);
			CreateServer(4000);
			CreateServer(10000);

			foreach(var time in LastTurnResultProcessRequests(6000))
				time.Should().BeCloseTo(TimeSpan.FromMilliseconds(4000), Epsilon);
		}
		
		[Test]
		public void ShouldReturnSuccessOnSecondCircleWhenLastReplicaIsGoodAndOthersAreSlow()
		{
			for (int i = 0; i < 3; i++)
				CreateServer(Slow);
			CreateServer(Fast);

			LastTurnResultProcessRequests(Timeout, 2).Last().Should().BeCloseTo(TimeSpan.FromMilliseconds(Fast), Epsilon);
		}
		
		[Test]
		public void ShouldReturnSuccessOnSecondCircleWhenLastReplicaIsGoodAndOthersAreBad()
		{
			for (int i = 0; i < 3; i++)
				CreateServer(1, status: 500);
			CreateServer(Fast);

			LastTurnResultProcessRequests(Timeout, 2).Last().Should().BeCloseTo(TimeSpan.FromMilliseconds(Fast), Epsilon);
		}
	}
}