import datetime
from unittest.mock import patch

from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from channels.layers import get_channel_layer
from channels.testing import WebsocketCommunicator
from django.db import transaction
from django.test import TransactionTestCase, override_settings
from django.utils import timezone

from users.models import User
from .models import Match
from .turn_feed import NationsTurnFeedConsumer, TURN_FEED_GROUP, pending_matches


class TurnFeedTests(TransactionTestCase):
    def setUp(self):
        self.kayser = User.objects.create(username='Kayser')
        self.other = User.objects.create(username='OtherPlayer')
        self.match = Match.objects.create(current_player=self.kayser, replay='started')

    def socket(self):
        return WebsocketCommunicator(NationsTurnFeedConsumer.as_asgi(), '/ws/nations/turns/')

    def test_snapshot_all_ongoing_and_heartbeat_read_only(self):
        Match.objects.filter(pk=self.match.pk).update(new_turn=timezone.now() - datetime.timedelta(days=30))
        Match.objects.create(current_player=self.kayser, replay='')
        Match.objects.create(current_player=self.kayser, replay='ended', game_over=True)
        Match.objects.create(replay='started')  # deleted_player placeholder
        with self.assertNumQueries(1):
            self.assertEqual(list(pending_matches().values_list('pk', flat=True)), [self.match.pk])

        async def check():
            socket = self.socket()
            self.assertTrue((await socket.connect())[0])
            self.assertEqual(await socket.receive_json_from(), {
                'type': 'snapshot', 'turns': [{'game': str(self.match.pk), 'player': 'Kayser'}]})
            await socket.send_json_to(None)
            self.assertEqual(await socket.receive_json_from(), {'type': 'pong'})
            await socket.send_json_to({'move': 'PASS'})
            self.assertEqual((await socket.receive_output())['code'], 1008)
            await socket.disconnect()

        async_to_sync(check)()
        self.match.refresh_from_db()
        self.assertEqual(self.match.replay, 'started')

    def test_new_game_is_broadcast_to_each_feed_connection(self):
        async def check():
            sockets = [self.socket(), self.socket()]
            for socket in sockets:
                await socket.connect()
                await socket.receive_json_from()
            match = await database_sync_to_async(Match.objects.create)(current_player_id=self.other.pk, replay='started')
            for socket in sockets:
                self.assertEqual(await socket.receive_json_from(), {
                    'type': 'turn', 'game': str(match.pk), 'player': 'OtherPlayer'})
                await socket.disconnect()
        async_to_sync(check)()

    def test_changes_completion_deletion_and_no_same_player_updates(self):
        @database_sync_to_async
        def change(**fields):
            match = Match.objects.get(pk=self.match.pk)
            for key, value in fields.items():
                setattr(match, key, value)
            match.save()

        async def check():
            socket = self.socket()
            await socket.connect()
            await socket.receive_json_from()
            await change(replay='another choice by same player')
            self.assertTrue(await socket.receive_nothing(timeout=0.05))
            await change(current_player_id=self.other.pk)
            self.assertEqual(await socket.receive_json_from(), {
                'type': 'turn', 'game': str(self.match.pk), 'player': 'OtherPlayer'})
            await change(game_over=True)
            self.assertEqual(await socket.receive_json_from(), {
                'type': 'turn', 'game': str(self.match.pk), 'player': None})
            await database_sync_to_async(Match.objects.filter(pk=self.match.pk).delete)()
            self.assertEqual((await socket.receive_json_from())['player'], None)
            await socket.disconnect()
            replacement = self.socket()
            await replacement.connect()
            self.assertEqual(await replacement.receive_json_from(), {'type': 'snapshot', 'turns': []})
            await replacement.disconnect()

        async_to_sync(check)()

    def test_queued_event_reads_latest_owner_instead_of_reverting_snapshot(self):
        async def check():
            socket = self.socket()
            await socket.connect()
            await socket.receive_json_from()
            await database_sync_to_async(Match.objects.filter(pk=self.match.pk).update)(current_player_id=self.other.pk)
            # Simulate an old queued marker: the consumer resolves current DB state.
            await get_channel_layer().group_send(TURN_FEED_GROUP, {
                'type': 'pending_turn_changed', 'game': str(self.match.pk)})
            self.assertEqual((await socket.receive_json_from())['player'], 'OtherPlayer')
            await socket.disconnect()

        async_to_sync(check)()

    def test_publish_only_after_commit_and_never_on_rollback(self):
        with patch('Nations.turn_feed.async_to_sync') as sender:
            with transaction.atomic():
                self.match.current_player = self.other
                self.match.save()
                sender.assert_not_called()
            self.assertEqual(sender.call_count, 1)
            sender.reset_mock()
            with self.assertRaises(ValueError):
                with transaction.atomic():
                    self.match.current_player = self.kayser
                    self.match.save()
                    raise ValueError('rollback')
            sender.assert_not_called()

    def test_broadcast_failure_does_not_break_saved_move(self):
        with patch('Nations.turn_feed.get_channel_layer', side_effect=RuntimeError('offline')):
            with self.assertLogs('Nations.turn_feed', level='ERROR'):
                self.match.current_player = self.other
                self.match.save()
        self.match.refresh_from_db()
        self.assertEqual(self.match.current_player_id, self.other.pk)

    @override_settings(NATIONS_TURN_FEED_ENABLED=False)
    def test_disabled_rejects_connections_and_adds_no_save_queries(self):
        async def check():
            socket = self.socket()
            self.assertFalse((await socket.connect())[0])
            await socket.disconnect()
        async_to_sync(check)()
        with self.assertNumQueries(1):
            self.match.current_player = self.other
            self.match.save()
