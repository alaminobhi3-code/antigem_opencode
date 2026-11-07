package com.example.gemini.data.agent.opencode

import android.content.Context
import com.example.gemini.data.agent.AgentChatBackend
import com.example.gemini.domain.model.AgentKind
import com.example.gemini.domain.model.ChatMessage
import com.example.gemini.domain.model.MessageRole
import com.example.gemini.domain.model.Conversation
import com.example.gemini.data.agent.ChatSessionStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import java.io.File
import java.util.UUID

/**
 * OpenCode chat backend for AntiGem (AndCode-native engine integration).
 * Directly invokes and coordinates the local OpenCode musl runner and MCP endpoints.
 */
class OpenCodeChatBackend(
    private val context: Context,
    private val scope: CoroutineScope,
    private val store: ChatSessionStore
) : AgentChatBackend {

    override val kind: AgentKind = AgentKind.OPENCODE

    private val _conversations = MutableStateFlow<List<Conversation>>(emptyList())
    override val conversations: StateFlow<List<Conversation>> = _conversations.asStateFlow()

    private val _currentConversation = MutableStateFlow<Conversation?>(null)
    override val currentConversation: StateFlow<Conversation?> = _currentConversation.asStateFlow()

    private val _messages = MutableStateFlow<List<ChatMessage>>(emptyList())
    override val messages: StateFlow<List<ChatMessage>> = _messages.asStateFlow()

    private val _isStreaming = MutableStateFlow(false)
    override val isStreaming: StateFlow<Boolean> = _isStreaming.asStateFlow()

    private val _isLoadingConversation = MutableStateFlow(false)
    override val isLoadingConversation: StateFlow<Boolean> = _isLoadingConversation.asStateFlow()

    private val _conversationError = MutableStateFlow<String?>(null)
    override val conversationError: StateFlow<String?> = _conversationError.asStateFlow()

    private var activeProcess: Process? = null

    override fun startNewChat() {
        val id = UUID.randomUUID().toString()
        val newConv = Conversation(
            id = id,
            title = "New OpenCode Chat",
            sessionId = id,
            agent = AgentKind.OPENCODE
        )
        _currentConversation.value = newConv
        store.currentConversation.value = newConv
        _messages.value = emptyList()
        store.messages.value = emptyList()
        _conversations.value = listOf(newConv) + _conversations.value.filter { it.id != id }
    }

    override fun selectConversation(id: String) {
        val found = _conversations.value.find { it.id == id }
        if (found != null) {
            _currentConversation.value = found
            store.currentConversation.value = found
        }
    }

    override fun sendMessage(content: String) {
        if (content.isBlank()) return
        val current = _currentConversation.value ?: run {
            startNewChat()
            _currentConversation.value!!
        }

        val userMsg = ChatMessage(
            id = UUID.randomUUID().toString(),
            conversationId = current.id,
            role = MessageRole.USER,
            content = content,
            createdAt = System.currentTimeMillis()
        )
        _messages.value = _messages.value + userMsg

        val assistantMsgId = UUID.randomUUID().toString()
        val assistantMsg = ChatMessage(
            id = assistantMsgId,
            conversationId = current.id,
            role = MessageRole.ASSISTANT,
            content = "Thinking...",
            createdAt = System.currentTimeMillis()
        )
        val updatedList = _messages.value + assistantMsg
        _messages.value = updatedList
        store.messages.value = updatedList
        _isStreaming.value = true

        scope.launch {
            try {
                val opencodeBin = File("/data/user/0/com.termux/files/home/.local/bin/opencode")
                if (!opencodeBin.exists()) {
                    updateAssistantMessage(assistantMsgId, "Error: OpenCode binary not found at ~/.local/bin/opencode")
                    _isStreaming.value = false
                    return@launch
                }

                val pb = ProcessBuilder(
                    opencodeBin.absolutePath,
                    "run",
                    content,
                    "--pure"
                )
                pb.directory(File(context.filesDir, "home/projects"))
                pb.environment()["HOME"] = "/data/user/0/com.termux/files/home"
                pb.redirectErrorStream(true)

                val process = pb.start()
                activeProcess = process

                val reader = process.inputStream.bufferedReader()
                val output = StringBuilder()
                var line: String? = reader.readLine()
                while (line != null) {
                    output.append(line).append("\n")
                    updateAssistantMessage(assistantMsgId, output.toString())
                    line = reader.readLine()
                }

                val exitCode = process.waitFor()
                if (output.isBlank()) {
                    updateAssistantMessage(assistantMsgId, if (exitCode == 0) "Completed" else "Process exited with code $exitCode")
                }
            } catch (e: Exception) {
                updateAssistantMessage(assistantMsgId, "Execution failed: ${e.message}")
            } finally {
                _isStreaming.value = false
                activeProcess = null
            }
        }
    }

    private fun updateAssistantMessage(id: String, newContent: String) {
        val updated = _messages.value.map { msg ->
            if (msg.id == id) msg.copy(content = newContent) else msg
        }
        _messages.value = updated
        store.messages.value = updated
    }

    override fun stopStreaming() {
        activeProcess?.destroy()
        activeProcess = null
        _isStreaming.value = false
    }

    override fun deleteConversation(id: String) {
        _conversations.value = _conversations.value.filter { it.id != id }
        if (_currentConversation.value?.id == id) {
            _currentConversation.value = null
            _messages.value = emptyList()
        }
    }

    override fun forkConversation(id: String) {}

    override fun retryConnections() {}

    override fun onAppForegrounded() {}
}
